"""The QPID service functionality."""
import logging
from time import sleep, time
from typing import Optional, Tuple

import requests
from slurm_rest import ApiClient  # type: ignore
from slurm_rest.api import SlurmApi  # type: ignore
from slurm_rest.rest import ApiException  # type: ignore
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from jobmon.server.squid_integration.squid_config import SQUIDConfig
from jobmon.server.squid_integration.slurm_maxrss_queue import MaxrssQ
from jobmon.server.squid_integration.squid_utils import QueuedTI

"""A squid to prevent Liming from modifying the wrong file.

                        ^
                      /   \
                     /     \
                    / @   @ \
                   /_   -   _\
                     |_____|
                    /  | |  \
                   /   | |   \
                       | |
"""

logger = logging.getLogger(__name__)

class IntegrationClusters:
    """A modified singleton to get clusters info that needs integration.

    This implementation assumes that the cluster ids and cluster type id
    never changes once the DB is created. Otherwise, the reboot the service.
    """
    _cluster_type_instance_dict = {"slurm": None,
                                  "uge": None}

    @staticmethod
    def get_cluster_type_requests_integration():
        return list(IntegrationClusters._cluster_type_instance_dict.keys())

    @staticmethod
    def get_instance(session, cluster_type: str):  # type: ignore
        if cluster_type not in IntegrationClusters._cluster_type_instance_dict.keys():
            return None
        if IntegrationClusters._cluster_type_instance_dict[cluster_type] is None:
            IntegrationClusters._cluster_type_instance_dict[cluster_type]\
                = IntegrationClusters()

            # get cluster type id
            sql = f"""SELECT id
                 FROM cluster_type
                 WHERE cluster_type.name = "{cluster_type}"
            """
            row = session.execute(sql).fetchone()
            IntegrationClusters._cluster_type_instance_dict[cluster_type]\
                .cluster_type_id = int(row["id"])

            # get cluster ids
            sql = f"""SELECT cluster.id 
                  FROM cluster, cluster_type
                  WHERE cluster.cluster_type_id = cluster_type.id
                  AND cluster_type.name = "{cluster_type}"
            """
            rows = session.execute(sql).fetchall()
            cluster_ids = [int(r["id"]) for r in rows]
            IntegrationClusters._cluster_type_instance_dict[cluster_type].cluster_ids = cluster_ids
        return IntegrationClusters._cluster_type_instance_dict[cluster_type]

    def __init__(self):
        """Don't call."""
        self.cluster_ids = []
        self.cluster_type_id = None


# slurm
def _get_slurm_api(item: QueuedTI) -> SlurmApi:
    pass


def _get_squid_resource(item: QueuedTI) -> dict:
    """Collect the Slurm reported resource usage for a given task instance.

       Return 5 values: cpu, mem, node, billing and runtime that are available from tres.
       For mem, also search the highest values within each step's tres.requested.max
       (only this way it will match results from "sacct -j nnnn --format="JobID,MaxRSS"").
       For runtime, get the total from the whole job, if 0, sum it up from the steps.
       """
    slurm_api = _get_slurm_api(item)

    usage_stats = {}
    for job in slurm_api.slurmdbd_get_job(item.distributor_id).jobs:
        for allocated in job.tres.allocated:
            if allocated["type"] in ("cpu", "node", "billing"):
                usage_stats[allocated["type"]] = allocated["count"]

        # the actual mem usage should have nothing to do with the allocation
        usage_stats["mem"] = 0
        for step in job.steps:
            for tres in step.tres.requested.max:
                if tres["type"] == "mem":
                    usage_stats["mem"] += tres["count"]

        usage_stats["runtime"] = job.time.total.microseconds / 1_000_000 + \
                                 job.time.total.seconds

        if usage_stats["runtime"] == 0:
            for step in job.steps:
                usage_stats["runtime"] += step.time.total.microseconds / 1_000_000 + \
                                          step.time.total.seconds

    # rename keys by copying
    # Guard against null returns
    if len(usage_stats) == 0:
        usage_stats["usage_str"] = "No usage stats received"
        usage_stats["wallclock"] = 0
        usage_stats["maxrss"] = 0
    else:
        usage_stats["usage_str"] = usage_stats.copy()
        usage_stats["wallclock"] = usage_stats.pop("runtime")
        # store B
        usage_stats["maxrss"] = usage_stats.pop("mem")
    logger.info(f"{item.distributor_id}: {usage_stats}")
    return usage_stats


# uge
def _get_qpid_response(distributor_id: int, qpid_uri_base: str) -> Tuple:
    qpid_api_url = f"{qpid_uri_base}/{distributor_id}"
    logger.info(qpid_api_url)
    resp = requests.get(qpid_api_url)
    if resp.status_code != 200:
        logger.info(
            f"The maxpss of {distributor_id} is not available. Put it back to the queue."
        )
        return (resp, None)
    else:
        maxpss = resp.json()["max_pss"]
        logger.debug(f"execution id: {distributor_id} maxpss: {maxpss}")
        return 200, maxpss


# common
def _get_cluster_ids(session, cluster_type: str) -> list:
    return IntegrationClusters.get_instance(session,
                                            cluster_type).cluster_ids


def _get_cluster_type_id(session, cluster_type: str) -> list:
    return IntegrationClusters.get_instance(session,
                                            cluster_type).cluster_type_id


def _update_maxrss_in_db(item: QueuedTI, session: Session,
                         qpid_uri_base: Optional[str]=None) -> bool:
    return_result = True
    logger.debug(str(item))
    try:
        if item.cluster_type_name == "uge":
            code, maxpss = _get_qpid_response(item.distributor_id, qpid_uri_base)
            if code != 200:
                logger.warning(f"Fail to get response from {qpid_uri_base}/{item.distributor_id} "
                               f"with {code}")
                return_result = False
            else:
                sql = f"UPDATE task_instance SET maxrss={maxpss} " \
                      f" WHERE id={item.task_instance_id}"
                session.execute(sql)
                session.commit()
        if item.cluster_type_name == "slurm":
            usage_stats = _get_squid_resource(item)
            rss = usage_stats["maxrss"]
            wallclock = usage_stats["wallclock"]
            # Doing single update instead of batch because if a batch update failed it's harder to
            # tell which task_instance has been updated
            sql = f"UPDATE task_instance SET maxrss={rss}, " \
                  f"wallclock={wallclock}" \
                  f" WHERE id={item.task_instance_id}"
            session.execute(sql)
            session.commit()
        if item.cluster_type_name == "dummy":
            # This is for testing only.
            # Production code should never access this block.
            sql = f"UPDATE task_instance SET maxrss=1314" \
                  f" WHERE id={item.task_instance_id}"
            session.execute(sql)
            session.commit()
    except Exception as e:
        logger.error(str(e))
        return_result = False
    finally:
        return return_result


def _get_completed_task_instance(starttime: float, session: Session) -> None:
    """Fetch completed SLURM task instances only."""
    sql = (
            "SELECT task_instance.id as id,  cluster_type.name as cluster_type "
            "from task_instance, cluster_type "
            'where task_instance.status not in ("B", "I", "R", "W") '
            "and UNIX_TIMESTAMP(task_instance.status_date) > {starttime} "
            "and task_instance.maxrss is null "
            "and task_instance.cluster_type_id = cluster_type.id".format(
                starttime=starttime
            )
        )
    rs = session.execute(sql).fetchall()
    session.commit()
    for r in rs:
        if r["cluster_type"] in IntegrationClusters.get_cluster_type_requests_integration():
            tid = int(r["id"])
            item = QueuedTI.create_instance_from_db(session, tid)
            if item:
                MaxrssQ.put(item)
            else:
                logger.warning(f"Fail to create QueuedTI for {tid}")


def _get_config() -> dict:
    config = SQUIDConfig.from_defaults()
    return {"conn_str": config.conn_str,
            "polling_interval": config.squid_polling_interval,
            "max_update_per_sec": config.squid_max_update_per_second,
            "qpid_uri_base": config.qpid_uri_base}


def _update_tis(max_update_per_sec: int, session: Session,
                qpid_uri_base: Optional[str] = None):
    for i in range(max_update_per_sec):
        r = MaxrssQ.get()
        if r is not None:
            (item, age) = r
            if _update_maxrss_in_db(item, session, qpid_uri_base):
                logger.info(f"Updated: {item}")
            else:
                MaxrssQ.put(item, age + 1)
                logger.warning(f"Failed to update db, "
                               f"put {item} back to the queue.")
        else:
            return

def maxrss_forever(init_time: int = 0) -> None:
    """A never stop method running in a thread that queries QPID.

    It constantly queries the maxpss value from qpid for completed jobmon jobs. If the maxpss
    is not found in qpid, put the execution id back to the queue.
    """
    # allow the service to decide the time to go back to fill maxrss/maxpss
    last_heartbeat = init_time
    vars_from_config = _get_config()
    eng = create_engine(vars_from_config["conn_str"], pool_recycle=200)
    Session = sessionmaker(bind=eng)
    session = Session()

    while MaxrssQ.keep_running:
        # Since there isn't a good way to specify the thread priority in Python,
        # put a sleep in each attempt to not overload the CPU.
        # The avg daily job instance is about 20k; thus, sleep(1) should be ok.
        sleep(1)
        # Update squid_max_update_per_second of jobs as defined in jobmon.cfg
        _update_tis(vars_from_config["max_update_per_sec"], session,
                    vars_from_config["qpid_uri_base"])

        # Query DB to add newly completed jobs to q and log q length
        current_time = time()
        if int(current_time - last_heartbeat) > vars_from_config["polling_interval"]:
            logger.info("MaxrssQ length: {}".format(MaxrssQ.get_size()))
            try:
                _get_completed_task_instance(last_heartbeat, session)
                logger.debug(f"Q length: {MaxrssQ.get_size()}")
            except Exception as e:
                logger.error(str(e))
            finally:
                last_heartbeat = current_time
    session.close()
