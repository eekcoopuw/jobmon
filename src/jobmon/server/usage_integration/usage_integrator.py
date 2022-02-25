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
from sqlalchemy.orm import Session, sessionmaker

from jobmon.server.usage_integration.config import UsageConfig
from jobmon.server.usage_integration.resilient_slurm_api import (
    ResilientSlurmApi as slurm,
)
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI

logger = logging.getLogger(__name__)


class UsageIntegrator:

    def __init__(self):
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

    @property
    def slurm_api(self):
        current_time = time()
        if self._slurm_api is None or \
                current_time - self.token_refresh_time > self.token_lifespan:
            # Need to refresh the api object
            newtoken = self.get_slurmtool_token()
            configuration = slurm_rest.Configuration(
                host=self.connection_parameters['slurm_rest_host'],
                api_key={
                    "X-SLURM-USER-NAME": self.user,
                    "X-SLURM-USER-TOKEN": newtoken,
                },
            )
            _slurm_api = slurm(slurm_rest.ApiClient(configuration))
            self._slurm_api = _slurm_api
            self.token_refresh_time = current_time
        return self._slurm_api

    @property
    def connection_parameters(self) -> Dict:
        if len(self._connection_params) == 0:
            # Ask the database for params, then cache
            sql = (
                "SELECT connection_parameters "
                "FROM cluster "
                f"WHERE name = 'slurm'")  # Hardcoded for now, hopefully not long lived
            row = self.session.execute(sql).fetchone()
            self.session.commit()
            if row:
                params = json.loads(row.connection_parameters)
                self._connection_params = params
                return params

    def get_slurmtool_token(self):
        slurm_api_host, slurm_token_url = self.connection_parameters['slurm_rest_host'], \
                                          self.connection_parameters['slurmtool_token_url']
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
        sql = (
            "SELECT task_instance.id as id,  "
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
            "".format(
                starttime=starttime
            )
        )
        task_instances = self.session.execute(sql).fetchall()
        self.session.commit()

        for ti in task_instances:
            if (ti.cluster_type == "UGE" and ti.maxpss is None) or \
                    (ti.cluster_type == "slurm" and ti.maxrss in (None, 0, -1)):
                queued_ti = QueuedTI(
                    task_instance_id=ti.id,
                    distributor_id=ti.distributor_id,
                    cluster_type_name=ti.cluster_type_name,
                    cluster_id=ti.cluster_id)
                UsageQ.put(queued_ti)

    def update_resources_in_db(self, task_instances: List[QueuedTI]):
        # Pull resource list from SQUID/QPID
        slurm_tasks = [ti for ti in task_instances if ti.cluster_type_name == 'slurm']
        uge_tasks = [ti for ti in task_instances if ti.cluster_type_name == 'UGE']

        self.update_slurm_resources(slurm_tasks)
        self.update_uge_resources(uge_tasks)

    def update_slurm_resources(self, tasks: List[QueuedTI]) -> None:
        # Unfortunately, need to fetch squid resources 1x1, until we get database access
        # or the slurm_rest package gets an option to call slurmdbd_get_jobs on a list of ids.

        # This loop is a prime candidate for an async refactor, but since the rest service
        # is the fragile point we don't want to overwhelm it.
        usage_stats = {task.task_instance_id:
                       _get_squid_resource(self.slurm_api, task.distributor_id)
                       for task in tasks}

        # If no resources were returned, add the failed TIs back to the queue
        for task in tasks:
            if usage_stats[task.task_instance_id] is None:
                usage_stats.pop(task.task_instance_id)
                task.age += 1
                UsageQ.put(task)

        # Attempt an insert, and update resources on duplicate primary key
        # There is a hypothetical max length of this query, limited by the value of
        # max_allowed_packet in the database.

        # The rough estimate is that each tuple is ~270 bytes, and the current config defaults
        # to a maximum of 100 task instances each time. Max packet size is ~1e9 in the DB.
        # So we probably aren't close to that limit.
        sql = (
            "INSERT INTO task_instance(id, maxrss, usage_str, task_id, status) "
            "VALUES {values} "
            "ON DUPLICATE KEY UPDATE "
            "maxrss=VALUES(maxrss), "
            "wallclock=VALUES(wallclock) "
        )

        # Note: the task_id and status attributes are mocked and not used. Required by the DB
        # since there are no defaults for those two columns. Never used since we should fail a
        # primary key uniqueness check on task instance ID
        values = ",".join(
            [str((tid, stats['maxrss'], stats['usage_str'], 1, 'D'))
             for tid, stats in usage_stats.items()])

        self.session.execute(sql.format(values=values))
        self.session.commit()

    def update_uge_resources(self, tasks: List[QueuedTI]) -> None:

        usage_stats = {
            task.task_instance_id:
            _get_qpid_response(task.distributor_id, self.config['qpid_base_uri'])
            for task in tasks
        }

        # If no resources were returned, add the failed TIs back to the queue
        for task in tasks:
            if usage_stats[task.task_instance_id] is None:
                usage_stats.pop(task.task_instance_id)
                task.age += 1
                UsageQ.put(task)

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
            [str((tid, usage_stats[tid], 1, 'D'))
             for tid, stats in usage_stats.items()])

        self.session.execute(sql.format(values=values))
        self.session.commit()


def _get_service_user_pwd(env_variable: str = "SVCSCICOMPCI_PWD") -> Optional[str]:
    return os.getenv(env_variable)


def _get_squid_resource(slurm_api: slurm, distributor_id: int) -> Optional[dict]:
    """Collect the Slurm reported resource usage for a given task instance.

    Return 5 values: cpu, mem, node, billing and runtime that are available from tres.
    For mem, also search the highest values within each step's tres.requested.max
    (only this way it will match results from "sacct -j nnnn --format="JobID,MaxRSS"").
    For runtime, get the total from the whole job, if 0, sum it up from the steps.
    """

    usage_stats = {}

    for job in slurm_api.slurmdbd_get_jobs(distributor_id).jobs:
        for allocated in job.tres.allocated:
            if allocated["type"] in ("cpu", "node", "billing"):
                usage_stats[allocated["type"]] = allocated["count"]

        # the actual mem usage should have nothing to do with the allocation
        usage_stats["mem"] = 0
        for step in job.steps:
            for tres in step.tres.requested.max:
                if tres["type"] == "mem":
                    usage_stats["mem"] += tres["count"]

        usage_stats["runtime"] = job.time.elapsed

    # rename keys by copying
    # Guard against null returns
    if len(usage_stats) == 0:
        return None
    else:
        usage_stats["usage_str"] = usage_stats.copy()
        usage_stats["wallclock"] = usage_stats.pop("runtime")
        # store B
        usage_stats["maxrss"] = usage_stats.pop("mem")
    logger.info(f"{distributor_id}: {usage_stats}")
    return usage_stats


# uge
def _get_qpid_response(distributor_id: int, qpid_uri_base: Optional[str]) -> Optional[int]:
    qpid_api_url = f"{qpid_uri_base}/{distributor_id}"
    resp = requests.get(qpid_api_url)
    if resp.status_code != 200:
        logger.info(
            f"The maxpss of {distributor_id} is not available. Put it back to the queue."
        )
        return None
    else:
        maxpss = resp.json()["max_pss"]
        logger.debug(f"execution id: {distributor_id} maxpss: {maxpss}")
        return maxpss


def _get_config() -> dict:
    config = UsageConfig.from_defaults()
    return {
        "conn_str": config.conn_str,
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
        task_instances = [UsageQ.get() for _ in range(integrator.config['max_update_per_sec'])]
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
