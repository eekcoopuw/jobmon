"""The QPID service functionality."""
from base64 import b64encode
import json
import logging
import os
from time import sleep, time
from typing import Any, Dict, List, Optional, Tuple

import requests
import slurm_rest  # type: ignore
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from jobmon.server.usage_integration.config import UsageConfig
from jobmon.server.usage_integration.resilient_slurm_api import (
    ResilientSlurmApi as slurm,
)
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI

logger = logging.getLogger(__name__)


class UsageIntegrator:
    """Retrieves usage data for jobs run on Slurm and UGE."""

    def __init__(self) -> None:
        """Initialization of the UsageIntegrator class."""
        self._connection_params = {}
        self._slurm_api = None
        self.heartbeat_time = 0
        self.token_refresh_time = 0
        self.token_lifespan = 86400  # TODO: make this configurable
        self.user = "svcscicompci"

        # Initialize config
        self.config = _get_config()

        # Initialize sqlalchemy session
        eng = create_engine(self.config["conn_str"], pool_recycle=200)
        session = sessionmaker(bind=eng)
        self.session = session()

        # Initialize sqlalchemy session for slurm_sdb
        eng_slurm_sdb = create_engine(self.config["conn_slurm_sdb_str"], pool_recycle=200)
        session_slurm_sdb = sessionmaker(bind=eng_slurm_sdb)
        self.session_slurm_sdb = session_slurm_sdb()

        self.tres_types = self._get_tres_types()
        logger.debug(f"tres types = {self.tres_types}")

    def _get_tres_types(self) -> Dict[str, int]:
        # get tres_type from tres_table
        tt: Dict[str, int] = {}
        sql_tres_table = "SELECT id, type " \
                         "FROM tres_table " \
                         "WHERE deleted = 0"
        tres_types = self.session_slurm_sdb.execute(sql_tres_table).all()
        self.session_slurm_sdb.commit()
        for tres_type in tres_types:
            tt[tres_type[1]] = tres_type[0]
        return tt

    @property
    def slurm_api(self) -> slurm:
        """Get Slurm api object if none or token needs to be refreshed."""
        current_time = time()
        if (
            self._slurm_api is None
            or current_time - self.token_refresh_time > self.token_lifespan
        ):
            # Need to refresh the api object
            newtoken = self.get_slurmtool_token()
            configuration = slurm_rest.Configuration(
                host=self.connection_parameters["slurm_rest_host"],
                api_key={
                    "X-SLURM-USER-NAME": self.user,
                    "X-SLURM-USER-TOKEN": newtoken,
                },
            )
            _slurm_api = slurm(slurm_rest.ApiClient(configuration, pool_threads=15))
            self._slurm_api = _slurm_api
            self.token_refresh_time = current_time
        return self._slurm_api

    @property
    def connection_parameters(self) -> Dict:
        """Cache the connection parameters of the Slurm cluster."""
        if len(self._connection_params) == 0:
            # Ask the database for params, then cache
            # Hardcoded cluster for now, hopefully not long lived
            sql = "SELECT connection_parameters FROM cluster WHERE name = 'slurm'"
            row = self.session.execute(sql).fetchone()
            self.session.commit()
            if row:
                params = json.loads(row.connection_parameters)
                self._connection_params = params
        return self._connection_params

    def get_slurmtool_token(self) -> str:
        """Get token for the Slurm tool."""
        _, slurm_token_url = (
            self.connection_parameters["slurm_rest_host"],
            self.connection_parameters["slurmtool_token_host"],
        )
        password = _get_service_user_pwd()
        if password is None:
            logger.warning(f"Fail to get the password for {self.user}")
            return None

        # get token
        auth_str = bytes(self.user + ":" + password, "utf-8")
        encoded_auth_str = b64encode(auth_str).decode("ascii")

        header = {
            "Authorization": f"Basic {encoded_auth_str}",
            "accept": "application/json",
            "Content-Type": "application/json",
        }

        payload = {"lifespan": 86400}
        response = requests.post(slurm_token_url, headers=header, json=payload)
        token = response.json()["access_token"]
        return token

    def populate_queue(self, starttime: float) -> None:
        """Collect jobs in terminal states that do not have resources; add to queue."""
        sql = (
            "SELECT task_instance.id as id,  "
            "task_instance.distributor_id as distributor_id, "
            "cluster_type.name as cluster_type, "
            "cluster.id as cluster_id, "
            "task_instance.maxrss as maxrss, "
            "task_instance.maxpss as maxpss "
            "FROM task_instance, cluster_type, cluster "
            'WHERE task_instance.status NOT IN ("B", "I", "R", "W") '
            "AND task_instance.distributor_id IS NOT NULL "
            "AND UNIX_TIMESTAMP(task_instance.status_date) > {starttime} "
            "AND (task_instance.maxrss IS NULL OR task_instance.maxpss IS NULL) "
            "AND task_instance.cluster_type_id = cluster_type.id "
            "AND cluster.cluster_type_id = cluster_type.id"
            "".format(starttime=starttime)
        )
        task_instances = self.session.execute(sql).fetchall()
        self.session.commit()
        for ti in task_instances:
            if (ti.cluster_type == "UGE" and ti.maxpss is None) or (
                ti.cluster_type in ("slurm", "dummy")
                and ti.maxrss in (None, 0, -1, "0", "-1")
            ):
                queued_ti = QueuedTI(
                    task_instance_id=ti.id,
                    distributor_id=ti.distributor_id,
                    cluster_type_name=ti.cluster_type,
                    cluster_id=ti.cluster_id,
                )
                UsageQ.put(queued_ti)

    def update_resources_in_db(self, task_instances: List[QueuedTI]) -> None:
        """Pull resource list from SQUID/QPID."""
        # Split tasks by cluster type
        slurm_tasks, uge_tasks, dummy_tasks = [], [], []
        for ti in task_instances:
            if ti.cluster_type_name == "slurm":
                slurm_tasks.append(ti)
            elif ti.cluster_type_name == "UGE":
                uge_tasks.append(ti)
            elif ti.cluster_type_name == "dummy":
                dummy_tasks.append(ti)

        if any(slurm_tasks):
            self.update_slurm_resources(slurm_tasks)
        if any(uge_tasks):
            self.update_uge_resources(uge_tasks)
        if any(dummy_tasks):
            self.update_dummy_resources(dummy_tasks)

    def update_slurm_resources(self, tasks: List[QueuedTI]) -> None:
        """Update resources for jobs that run on the Slurm cluster.

        """
        usage_stats = self._get_squid_resource_via_slurm_sdb(task_instances=tasks)

        # If no resources were returned, add the failed TIs back to the queue
        for task in tasks:
            try:
                resources = usage_stats[task]
            except KeyError:
                continue
            if resources is None:
                usage_stats.pop(task)
                task.age += 1
                UsageQ.put(task, task.age)

        if len(usage_stats) == 0:
            return  # No values to update

        # Attempt an insert, and update resources on duplicate primary key
        # There is a hypothetical max length of this query, limited by the value of
        # max_allowed_packet in the database.

        # The rough estimate is that each tuple is ~270 bytes, and the current config defaults
        # to a maximum of 100 task instances each time. Max packet size is ~1e9 in the DB.
        # So we probably aren't close to that limit.
        sql = (
            "INSERT INTO task_instance(id, maxrss, wallclock, task_id, status) "
            "VALUES {values} "
            "ON DUPLICATE KEY UPDATE "
            "maxrss=VALUES(maxrss), "
            "wallclock=VALUES(wallclock) "
        )

        # Note: the task_id and status attributes are mocked and not used. Required by the DB
        # since there are no defaults for those two columns. Never used since we should fail a
        # primary key uniqueness check on task instance ID
        values = ",".join(
            [
                str((ti.task_instance_id, stats["maxrss"], stats["wallclock"], 1, "D"))
                for ti, stats in usage_stats.items()
            ]
        )

        self.session.execute(sql.format(values=values))
        self.session.commit()

    def update_uge_resources(self, tasks: List[QueuedTI]) -> None:
        """Update the resources for jobs that were run on the UGE cluster."""
        usage_stats = {
            task: _get_qpid_response(task.distributor_id, self.config["qpid_uri_base"])[
                1
            ]
            for task in tasks
        }

        # If no resources were returned, add the failed TIs back to the queue
        for task in tasks:
            try:
                resources = usage_stats[task]
            except KeyError:
                continue
            if resources is None:
                usage_stats.pop(task)
                task.age += 1
                UsageQ.put(task, task.age)

        if len(usage_stats) == 0:
            return  # No values to update

        # Attempt an insert, and update resources on duplicate primary key
        # There is a hypothetical max length of this query, limited by the value of
        # max_allowed_packet in the database.

        # The rough estimate is that each tuple is ~270 bytes, and the current config defaults
        # to a maximum of 100 task instances each time. Max packet size is ~1e9 in the DB.
        # So we probably aren't close to that limit.
        sql = (
            "INSERT INTO task_instance(id, maxrss, task_id, status) "
            "VALUES {values} "
            "ON DUPLICATE KEY UPDATE "
            "maxrss=VALUES(maxrss), "
            "usage_str=VALUES(usage_str)"
        )

        # Note: the task_id and status attributes are mocked and not used. Required by the DB
        # since there are no defaults for those two columns. Never used since we should fail a
        # primary key uniqueness check on task instance ID
        values = ",".join(
            [
                str((ti.task_instance_id, maxrss, 1, "D"))
                for ti, maxrss in usage_stats.items()
            ]
        )

        self.session.execute(sql.format(values=values))
        self.session.commit()

    def update_dummy_resources(self, task_instances: List[QueuedTI]) -> None:
        """Set the dummy resource values for maxrss."""
        # Hardcode a value for maxrss, just for unit testing
        values = ",".join([str(ti.task_instance_id) for ti in task_instances])

        sql = "UPDATE task_instance SET maxrss=1314 WHERE id IN ({})"
        self.session.execute(sql.format(values))
        self.session.commit()


def _get_service_user_pwd(env_variable: str = "SVCSCICOMPCI_PWD") -> Optional[str]:
    return os.getenv(env_variable)


def _get_squid_resource_via_slurm_sdb(session: Session,
                                      tres_types: Dict[str, int],
                                      task_instances: List[QueuedTI]
                                      ) -> Dict[QueuedTI, Dict[str, Optional[Any]]]:
    """Collect the Slurm reported resource usage for a given list of task instances
       via slurm_sdb.
    Return 5 values: cpu, mem, node, billing and elapsed that are available from
    slurm_sdb.
    """
    import ast

    all_usage_stats: Dict[QueuedTI, Dict[str, Optional[Any]]] = {}

    # mapping of distributor_id and task instance
    dict_dist_ti: Dict[int, QueuedTI] = {}
    # with distributor_id as the key
    raw_usage_stats: Dict[int, Dict[str, Optional[Any]]] = {}

    distributor_ids: List[int] = [task_instance.distributor_id
                                  for task_instance in task_instances]
    for task_instance in task_instances:
        dict_dist_ti[task_instance.distributor_id] = task_instance

    # get job level data
    sql_job = "SELECT job.id_job, job.time_end - job.time_start AS elapsed, tres_alloc " \
              "FROM general_job_table job " \
              "WHERE job.deleted = 0 AND " \
              "job.id_job IN :job_ids"
    jobs = session.execute(sql_job, {"job_ids": distributor_ids}).all()
    session.commit()
    for job in jobs:
        job_stats: Dict[str, Optional[Any]] = {"runtime": job[1], "mem": 0}
        tres_alloc = ast.literal_eval("{" + job[2].replace("=", ":") + "}")
        for tres_type_name in ["cpu", "node", "billing"]:
            job_stats[tres_type_name] = tres_alloc[tres_types[tres_type_name]]
        raw_usage_stats[job[0]] = job_stats

    # get step level data
    sql_step = "SELECT job.id_job, step.tres_usage_in_max " \
               "FROM general_step_table step " \
               "INNER JOIN general_job_table job ON step.job_db_inx = job.job_db_inx " \
               "WHERE step.deleted = 0 AND job.deleted = 0 " \
               "AND job.id_job IN :job_ids"

    steps = session.execute(sql_step, {"job_ids": distributor_ids}).all()
    session.commit()
    for step in steps:
        job_stats: Dict[str, Optional[Any]] = raw_usage_stats.get(step[0], None)
        if job_stats is None:
            continue
        tres_usage_in_max = ast.literal_eval("{" + step[1].replace("=", ":") + "}")
        job_stats["mem"] += tres_usage_in_max.get(tres_types["mem"], 0)

    for k, v in raw_usage_stats.items():
        v["usage_str"] = v.copy()
        v["wallclock"] = v.pop("runtime")
        v["maxrss"] = v.pop("mem")
        logger.info(f"{k}: {v}")
        all_usage_stats[dict_dist_ti[k]] = v

    return all_usage_stats


# Old routine of using Slurm_API for usage_stats
# def _get_squid_resource(
#     slurm_api: slurm, task_instances: List[QueuedTI]
# ) -> Optional[dict]:
#     """Collect the Slurm reported resource usage for a given list of task instances.
#
#     Return 5 values: cpu, mem, node, billing and runtime that are available from tres.
#     For mem, also search the highest values within each step's tres.requested.max
#     (only this way it will match results from "sacct -j nnnn --format="JobID,MaxRSS"").
#     For runtime, get the total from the whole job, if 0, sum it up from the steps.
#     """
#     all_usage_stats = {}
#     # Return futures since async_req=True is passed
#     job_futures = [
#         slurm_api.slurmdbd_get_job(ti.distributor_id) for ti in task_instances
#     ]
#     jobs = [j.get() for j in job_futures]
#     for task_instance, j in zip(task_instances, jobs):
#         usage_stats = {}
#         job = j.jobs[
#             0
#         ]  # Always length 1 I believe? Unless it's an array task potentially?
#         for allocated in job.tres.allocated:
#             if allocated["type"] in ("cpu", "node", "billing"):
#                 usage_stats[allocated["type"]] = allocated["count"]
#
#         # the actual mem usage should have nothing to do with the allocation
#         usage_stats["mem"] = 0
#         for step in job.steps:
#             for tres in step.tres.requested.max:
#                 if tres["type"] == "mem":
#                     usage_stats["mem"] += tres["count"]
#
#         usage_stats["runtime"] = job.time.elapsed
#
#         # rename keys by copying
#         # Guard against null returns
#         if len(usage_stats) == 0:
#             usage_stats = None
#         else:
#             usage_stats["usage_str"] = usage_stats.copy()
#             usage_stats["wallclock"] = usage_stats.pop("runtime")
#             # store B
#             usage_stats["maxrss"] = usage_stats.pop("mem")
#         logger.info(f"{task_instance.distributor_id}: {usage_stats}")
#         all_usage_stats[task_instance] = usage_stats
#
#     return all_usage_stats
#
#
# uge
def _get_qpid_response(distributor_id: int, qpid_uri_base: Optional[str]) -> Tuple:
    qpid_api_url = f"{qpid_uri_base}/{distributor_id}"
    resp = requests.get(qpid_api_url)
    if resp.status_code != 200:
        logger.info(
            f"The maxpss of {distributor_id} is not available. Put it back to the queue."
        )
        return resp, None
    else:
        maxpss = resp.json()["max_pss"]
        logger.debug(f"execution id: {distributor_id} maxpss: {maxpss}")
        return 200, maxpss


def _get_config() -> dict:
    config = UsageConfig.from_defaults()
    return {
        "conn_str": config.conn_str,
        "conn_slurm_sdb_str": config.conn_slurm_sdb_str,
        "polling_interval": config.squid_polling_interval,
        "max_update_per_sec": config.squid_max_update_per_second,
        "qpid_uri_base": config.qpid_uri_base,
    }


def q_forever(init_time: float = 0) -> None:
    """A never stop method running in a thread that queries QPID.

    It constantly queries the maxpss value from qpid for completed jobmon jobs. If the maxpss
    is not found in qpid, put the execution id back to the queue.
    """
    # allow the service to decide the time to go back to fill maxrss/maxpss
    last_heartbeat = init_time
    integrator = UsageIntegrator()

    while UsageQ.keep_running:
        # Since there isn't a good way to specify the thread priority in Python,
        # put a sleep in each attempt to not overload the CPU.
        # The avg daily job instance is about 20k; thus, sleep(1) should be ok.
        sleep(1)
        # Update squid_max_update_per_second of jobs as defined in jobmon.cfg
        task_instances = [
            UsageQ.get() for _ in range(integrator.config["max_update_per_sec"])
        ]
        # If the queue is empty, drop the None entries
        task_instances = [t for t in task_instances if t is not None]

        integrator.update_resources_in_db(task_instances)

        # Query DB to add newly completed jobs to q and log q length
        current_time = time()
        if int(current_time - last_heartbeat) > integrator.config["polling_interval"]:
            logger.info("UsageQ length: {}".format(UsageQ.get_size()))
            try:
                integrator.populate_queue(last_heartbeat)
                logger.debug(f"Q length: {UsageQ.get_size()}")
            except Exception as e:
                logger.error(str(e))
            finally:
                last_heartbeat = current_time
    integrator.session.close()
