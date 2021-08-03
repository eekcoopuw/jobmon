"""The QPID service functionality."""
import logging
from time import sleep, time
from typing import Tuple

from jobmon.server.qpid_integration.maxpss_queue import MaxpssQ
from jobmon.server.qpid_integration.qpid_config import QPIDConfig
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


logger = logging.getLogger(__name__)


config = QPIDConfig.from_defaults()


def _get_pulling_interval() -> int:
    """This method returns the current app. The main purpose is for easy patch in testing."""
    return config.qpid_polling_interval


def _update_maxpss_in_db(ex_id: int, pss: int, session: Session) -> bool:
    try:
        # Doing single update instead of batch because if a banch update failed it's harder to
        # tell which task_instance has been updated
        sql = f"UPDATE task_instance SET maxpss={pss} WHERE executor_id={ex_id}"
        session.execute(sql)
        session.commit()
        session.close()
        return True
    except Exception as e:
        logger.error(str(e))
        return False


def _get_qpid_response(ex_id: int) -> Tuple:
    qpid_api_url = f"{config.qpid_uri}/{config.qpid_cluster}/jobmaxpss/{ex_id}"
    logger.info(qpid_api_url)
    resp = requests.get(qpid_api_url)
    if resp.status_code != 200:
        logger.info(f"The maxpss of {ex_id} is not available. Put it back to the queue.")
        return (resp, None)
    else:
        maxpss = resp.json()["max_pss"]
        logger.debug(f"execution id: {ex_id} maxpss: {maxpss}")
        return (200, maxpss)


def _get_completed_task_instance(starttime: float, session: Session) -> None:
    sql = "SELECT executor_id from task_instance " \
          "where status not in (\"B\", \"I\", \"R\", \"W\") " \
          "and UNIX_TIMESTAMP(status_date) > {} " \
          "and maxpss is null".format(starttime)
    rs = session.execute(sql).fetchall()
    session.commit()
    session.close()
    for r in rs:
        MaxpssQ().put(int(r[0]))


def maxpss_forever() -> None:
    """A never stop method running in a thread that queries QPID.

    It constantly queries the maxpss value from qpid for completed jobmon jobs. If the maxpss
    is not found in qpid, put the execution id back to the queue.
    """
    eng = create_engine(config.conn_str, pool_recycle=200)
    Session = sessionmaker(bind=eng)
    session = Session()
    last_heartbeat = time()
    while MaxpssQ.keep_running:
        # Since there isn't a good way to specify the thread priority in Python,
        # put a sleep in each attempt to not overload the CPU.
        # The avg daily job instance is about 20k; thus, sleep(1) should be ok.
        sleep(1)
        # Update qpid_max_update_per_second of jobs as defined in jobmon.cfg
        for i in range(config.qpid_max_update_per_second):
            r = MaxpssQ().get()
            if r is not None:
                (ex_id, age) = r
                (status_code, maxpss) = _get_qpid_response(ex_id)
                if status_code != 200:
                    # Maxpss not ready
                    MaxpssQ().put(ex_id, age + 1)
                    logger.info("Maxpss is not ready. Put {} back to the queue.".format(ex_id))
                else:
                    if _update_maxpss_in_db(ex_id, maxpss, session):
                        logger.info(f"Updated execution id: {ex_id} maxpss: {maxpss}")
                    else:
                        MaxpssQ().put(ex_id, age + 1)
                        logger.warning(f"Failed to update db, put {ex_id} back to the queue.")
        # Query DB to add newly completed jobs to q and log q length every 30 minute
        current_time = time()
        if int(current_time - last_heartbeat) > _get_pulling_interval():
            logger.info("MaxpssQ length: {}".format(MaxpssQ().get_size()))
            try:
                _get_completed_task_instance(last_heartbeat, session)
            except Exception as e:
                logger.error(str(e))
            finally:
                last_heartbeat = current_time
