"""The QPID service functionality."""
import logging
from time import sleep, time
from typing import Tuple

import requests
from slurm_rest import ApiClient  # type: ignore
from slurm_rest.api import SlurmApi  # type: ignore
from slurm_rest.rest import ApiException  # type: ignore
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from jobmon.server.qpid_integration.slurm_maxrss_queue import MaxrssQ
from jobmon.server.qpid_integration.qpid_config import QPIDConfig

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

class SlurmClusters:
    """A singleton to get slurm cluster ids.

    This implementation assumes that the slurm cluster ids and
    cluster type id never changes once the DB is created. Otherwise,
    the service requires a reboot.
    """
    _instance = None
    _cluster_type_name = "slurm"

    @staticmethod
    def get_instance(session):
        if SlurmClusters._instance is None:
            SlurmClusters._instance = SlurmClusters()

            # get cluster type id
            sql = f"""SELECT id
                 FROM cluster_type
                 WHERE cluster_type.name = "{SlurmClusters._cluster_type_name}"
            """
            row = session.execute(sql).fetchone()
            SlurmClusters._instance.cluster_type_id = int(row["id"])

            # get cluster ids
            sql = f"""SELECT cluster.id 
                  FROM cluster, cluster_type
                  WHERE cluster.cluster_type_id = cluster_type.id
                  AND cluster_type.name = "{SlurmClusters._cluster_type_name}"
            """
            rows = session.execute(sql).fetchall()
            cluster_ids = [int(r["id"]) for r in rows]
            SlurmClusters._instance.cluster_ids = cluster_ids
        return SlurmClusters._instance

    def __init__(self):
        """Don't call."""
        self.cluster_ids = []
        self.cluster_type_id = None


def _get_slurm_api() -> SlurmApi:
    pass


def _get_slurm_cluster_ids(session) -> list:
    return SlurmClusters.get_instance(session).cluster_ids


def _get_slurm_cluster_type_id(session) -> list:
    return SlurmClusters.get_instance(session).cluster_type_id


def _update_maxrss_in_db(distributor_id: int, session: Session) -> bool:
    try:
        usage_stats = _get_squid_resource(distributor_id)
        rss = usage_stats["maxrss"]
        wallclock = usage_stats["wallclock"]
        # Doing single update instead of batch because if a batch update failed it's harder to
        # tell which task_instance has been updated
        sql = f"UPDATE task_instance SET maxrss={rss}, wallclock={wallclock}" \
              f" WHERE distributor_id={distributor_id}"
        session.execute(sql)
        session.commit()
        session.close()
        return True
    except Exception as e:
        logger.error(str(e))
        return False


def _get_squid_resource(distributor_id: int) -> dict:
    """Collect the Slurm reported resource usage for a given task instance.

       Return 5 values: cpu, mem, node, billing and runtime that are available from tres.
       For mem, also search the highest values within each step's tres.requested.max
       (only this way it will match results from "sacct -j nnnn --format="JobID,MaxRSS"").
       For runtime, get the total from the whole job, if 0, sum it up from the steps.
       """
    slurm_api = _get_slurm_api()

    usage_stats = {}

    for job in slurm_api.slurmdbd_get_job(distributor_id).jobs:
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
    logger.info(f"{distributor_id}: {usage_stats}")
    return usage_stats


def _get_completed_task_instance(starttime: float, session: Session) -> None:
    """Fetch completed SLURM task instances only."""
    slurm_cluster_type_ids = _get_slurm_cluster_type_id(session)
    if slurm_cluster_type_ids:
        sql = (
            "SELECT distributor_id from task_instance "
            'where status not in ("B", "I", "R", "W") '
            "and UNIX_TIMESTAMP(status_date) > {starttime} "
            "and maxrss is null "
            "and cluster_type_id = {cluster_type_id}".format(
                starttime=starttime,
                cluster_type_id=slurm_cluster_type_ids
            )
        )
        rs = session.execute(sql).fetchall()
        session.commit()
        session.close()
        for r in rs:
            MaxrssQ().put(int(r[0]))


def _get_config() -> dict:
    config = QPIDConfig.from_defaults()
    return {"conn_str": config.conn_str,
            "polling_interval": config.squid_polling_interval,
            "max_update_per_sec": config.squid_max_update_per_second}


def maxrss_forever() -> None:
    """A never stop method running in a thread that queries QPID.

    It constantly queries the maxpss value from qpid for completed jobmon jobs. If the maxpss
    is not found in qpid, put the execution id back to the queue.
    """
    vars_from_config = _get_config()
    eng = create_engine(vars_from_config["conn_str"], pool_recycle=200)
    Session = sessionmaker(bind=eng)
    session = Session()
    last_heartbeat = time()
    while MaxrssQ.keep_running:
        # Since there isn't a good way to specify the thread priority in Python,
        # put a sleep in each attempt to not overload the CPU.
        # The avg daily job instance is about 20k; thus, sleep(1) should be ok.
        sleep(1)
        # Update qpid_max_update_per_second of jobs as defined in jobmon.cfg
        for i in range(vars_from_config["max_update_per_sec"]):
            r = MaxrssQ().get()
            if r is not None:
                (distributor_id, age) = r
                if _update_maxrss_in_db(distributor_id, session):
                    logger.info(f"Updated execution id: {distributor_id}")
                else:
                    MaxrssQ().put(distributor_id, age + 1)
                    logger.warning(f"Failed to update db, put {distributor_id} back to the queue.")
        # Query DB to add newly completed jobs to q and log q length every 30 minute
        current_time = time()
        if int(current_time - last_heartbeat) > vars_from_config["polling_interval"]:
            logger.info("MaxpssQ length: {}".format(MaxrssQ().get_size()))
            try:
                _get_completed_task_instance(last_heartbeat, session)
            except Exception as e:
                logger.error(str(e))
            finally:
                last_heartbeat = current_time