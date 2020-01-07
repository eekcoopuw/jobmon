from time import sleep, time
import requests
from flask_sqlalchemy import SQLAlchemy

from jobmon.server.server_logging import jobmonLogging as logging
from jobmon import config
from jobmon.server.integration.qpid.maxpss_queue import MaxpssQ
from jobmon.server import app

logger = logging.getLogger(__name__)


class _App:
    _app = None

    @staticmethod
    def get_app():
        if _App._app is None:
            _App._app = app
        return _App._app


def _get_current_app():
    """This method returns the current app. The main purpose is for easy patch in testing."""
    return _App.get_app()


def _update_maxpss_in_db(ex_id: int, pss: int):
    jobmon_api_url = f"http://{config.jobmon_server_sqdn}:{config.jobmon_service_port}/job_instance/{ex_id}/maxpss/{pss}"
    logger.info(jobmon_api_url)
    resp = requests.post(jobmon_api_url)
    if resp.status_code == 200:
        return True
    if resp.status_code == 500:
        logger.error(resp.json()["message"])
    return False


def _get_qpid_response(ex_id, age):
    qpid_api_url = f"{config.qpid_uri}/{config.qpid_cluster}/jobmaxpss/{ex_id}"
    logger.info(qpid_api_url)
    resp = requests.get(qpid_api_url)
    if resp.status_code != 200:
        logger.info("The maxpss of {} is not available. Put it back to the queue.".format(ex_id))
        return (resp, None)
    else:
        maxpss = resp.json()["max_pss"]
        logger.debug(f"execution id: {ex_id} maxpss: {maxpss}")
        return (200, maxpss)


def _get_completed_job_instance(starttime: float):
    db = SQLAlchemy( _get_current_app())
    sql = "SELECT executor_id from job_instance " \
          "where status not in (\"B\", \"I\", \"R\", \"W\") " \
          "and UNIX_TIMESTAMP(status_date) > {} " \
          "and maxpss is null".format(starttime)
    rs = db.session.execute(sql).fetchall()
    db.session.commit()
    for r in rs:
        MaxpssQ().put(int(r[0]))

def maxpss_forever():
    """A never stop method running in a thread to constantly query the maxpss value from qpid for completed jobmon jobs.
       If the maxpss is not found in qpid, put the execution id back to the queue.
    """
    last_heartbeat = time()
    while MaxpssQ.keep_running:
        # Since there isn't a good way to specify the thread priority in Python,
        # put a sleep in each attempt to not overload the CPU.
        # The avg daily job instance is about 20k; thus, sleep(1) should be ok.
        sleep(1)
        r = MaxpssQ().get()
        if r is not None:
            (ex_id, age) = r
            (status_code, maxpss) = _get_qpid_response(ex_id, age)
            if status_code != 200:
                # Maxpss not ready
                MaxpssQ().put(ex_id, age + 1)
                logger.info("Maxpss is not ready. Put {} back to the queue.".format(ex_id))
            else:
                if _update_maxpss_in_db(ex_id, maxpss):
                    logger.info(f"Updated execution id: {ex_id} maxpss: {maxpss}")
                else:
                    MaxpssQ().put(ex_id, age + 1)
                    logger.warning(f"Failed to update db, put {ex_id} back to the queue.")
        # Query DB to add newly completed jobs to q and log q length every 30 minute
        current_time = time()
        if int(current_time - last_heartbeat) > 1800:
            logger.info("MaxpssQ length: {}".format(MaxpssQ().get_size()))
            try:
                _get_completed_job_instance(last_heartbeat)
            except Exception as e:
                logger.error(str(e))
            finally:
                last_heartbeat = current_time
