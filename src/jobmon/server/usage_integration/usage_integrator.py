import ast
import datetime
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import slurm_rest  # type: ignore # noqa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from jobmon.server.usage_integration.config import UsageConfig
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI

logger = logging.getLogger(__name__)


class UsageIntegrator:
    """Retrieves usage data for jobs run on Slurm and UGE."""

    def __init__(self, config: UsageConfig = None) -> None:
        """Initialization of the UsageIntegrator class."""
        self.heartbeat_time = 0

        # Initialize config
        self.config = _get_config(config)

        # Initialize sqlalchemy session
        eng = create_engine(self.config["conn_str"], pool_recycle=200)
        session = sessionmaker(bind=eng)
        self.session = session()

        # Initialize sqlalchemy session for slurm_sdb
        eng_slurm_sdb = create_engine(self.config["conn_slurm_sdb_str"],
                                      pool_recycle=200, pool_pre_ping=True)
        session_slurm_sdb = sessionmaker(bind=eng_slurm_sdb)
        self.session_slurm_sdb = session_slurm_sdb()

        self.tres_types = self._get_tres_types()
        logger.debug(f"tres types = {self.tres_types}")

        # Initialize empty queue-cluster mapping, to be populated and cached on startup
        self._queue_cluster_map: Optional[Dict] = None
        self._integrator_retire_age: int = int(os.getenv("INTEGRATOR_RETIRE_AGE")) \
            if os.getenv("INTEGRATOR_RETIRE_AGE") else 0

    @property
    def integrator_retire_age(self) -> int:
        return self._integrator_retire_age

    @property
    def queue_cluster_map(self) -> Dict:
        """An in-memory cache of the mapping of queue id to cluster and cluster type id."""
        if self._queue_cluster_map is None:
            self._queue_cluster_map = {}
            get_map_query = (
                "SELECT queue.id AS queue_id, cluster.id AS cluster_id, cluster_type.name "
                "FROM queue "
                "JOIN cluster ON queue.cluster_id = cluster.id "
                "JOIN cluster_type ON cluster.cluster_type_id = cluster_type.id"
            )

            mapping_res = self.session.execute(get_map_query).all()
            for queue in mapping_res:
                self._queue_cluster_map[queue.queue_id] = (queue.cluster_id, queue.name)

        return self._queue_cluster_map

    def _get_tres_types(self) -> Dict[str, int]:
        # get tres_type from tres_table
        tt: Dict[str, int] = {}
        sql_tres_table = "SELECT id, type " "FROM tres_table " "WHERE deleted = 0"
        tres_types = self.session_slurm_sdb.execute(sql_tres_table).all()
        self.session_slurm_sdb.commit()
        for tres_type in tres_types:
            tt[tres_type[1]] = tres_type[0]
        return tt

    def populate_queue(self, starttime: datetime.datetime) -> None:
        """Collect jobs in terminal states that do not have resources; add to queue."""
        sql = (
            "SELECT task_instance.id as id,  "
            "task_instance.distributor_id as distributor_id, "
            "task_resources.queue_id as queue_id, "
            "task_instance.maxrss as maxrss, "
            "task_instance.maxpss as maxpss "
            "FROM task_instance, task_resources "
            'WHERE task_instance.status NOT IN ("B", "I", "R", "W") '
            "AND task_instance.distributor_id IS NOT NULL "
            "AND task_instance.status_date > '{starttime}' "
            "AND (task_instance.maxrss IS NULL OR task_instance.maxpss IS NULL) "
            "AND task_instance.task_resources_id = task_resources.id "
            "".format(starttime=str(starttime))
        )
        task_instances = self.session.execute(sql).fetchall()
        self.session.commit()
        for ti in task_instances:
            cluster_id, cluster_name = self.queue_cluster_map[ti.queue_id]
            if (
                cluster_name in ("slurm", "dummy")
                and ti.maxrss in (None, 0, -1, "0", "-1")
            ):
                queued_ti = QueuedTI(
                    task_instance_id=ti.id,
                    distributor_id=ti.distributor_id,
                    cluster_type_name=cluster_name,
                    cluster_id=cluster_id,
                )
                UsageQ.put(queued_ti)

    def update_resources_in_db(self, task_instances: List[QueuedTI]) -> None:
        """Pull resource list from the SLURM accounting database."""
        # Split tasks by cluster type
        slurm_tasks, dummy_tasks = [], []
        for ti in task_instances:
            if ti.cluster_type_name == "slurm":
                slurm_tasks.append(ti)
            elif ti.cluster_type_name == "dummy":
                dummy_tasks.append(ti)

        if any(slurm_tasks):
            self.update_slurm_resources(slurm_tasks)
        if any(dummy_tasks):
            self.update_dummy_resources(dummy_tasks)

    def update_slurm_resources(self, tasks: List[QueuedTI]) -> None:
        """Update resources for jobs that run on the Slurm cluster."""
        usage_stats = _get_slurm_resource_via_slurm_sdb(
            session=self.session_slurm_sdb,
            tres_types=self.tres_types,
            task_instances=tasks,
        )
        # If no resources were returned, add the failed TIs back to the queue
        for task in tasks:
            resources = usage_stats.get(task)

            if resources is None:
                try:
                    usage_stats.pop(task)
                    task.age += 1
                    # discard older than 10 tasks when never_retire is False
                    if self.integrator_retire_age <= 0 \
                            or task.age < self.integrator_retire_age:
                        logger.info(f"Put {task.task_instance_id} back to the queue with "
                                    f"age {task.age}")
                        UsageQ.put(task, task.age)
                    else:
                        logger.info(f"Retire {task.task_instance_id} at age {task.age}")
                except Exception as e:
                    # keeps integrator running with failures
                    logger.warning(e)

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

    def update_dummy_resources(self, task_instances: List[QueuedTI]) -> None:
        """Set the dummy resource values for maxrss."""
        # Hardcode a value for maxrss, just for unit testing
        values = ",".join([str(ti.task_instance_id) for ti in task_instances])

        sql = "UPDATE task_instance SET maxrss=1314 WHERE id IN ({})"
        self.session.execute(sql.format(values))
        self.session.commit()


def _get_slurm_resource_via_slurm_sdb(session: Session,
                                      tres_types: Dict[str, int],
                                      task_instances: List[QueuedTI]
                                      ) -> Dict[QueuedTI, Dict[str, Optional[Any]]]:
    """Collect the Slurm reported resource usage for a given list of task instances.

    Using slurm_sdb.
    Return 5 values: cpu, mem, node, billing and elapsed that are available from
    slurm_sdb.
    """
    all_usage_stats: Dict[QueuedTI, Dict[str, Optional[Any]]] = {}

    # mapping of distributor_id and task instance
    dict_dist_ti: Dict[str, QueuedTI] = {}
    # with distributor_id as the key
    raw_usage_stats: Dict[str, Dict[str, Optional[Any]]] = {}

    nonarray_distributor_ids: List[str] = []
    array_distributor_ids: List[Tuple[str]] = []

    for ti in task_instances:
        # Need to generate the id_job variable if the task is an array task
        distributor_id_split = str(ti.distributor_id).split("_")
        if len(distributor_id_split) == 2:
            # Array tasks (slurm) have a distributor_id value like "xyz_abc"
            # non array tasks have a distributor_id value like "xyz"
            # The split call should give a list of length 2 or 1 depending on whether it's
            # an array task or not.

            # We have to assume a cluster-specific implementation here. The
            # jobmon-slurm plugin and the SLURM cluster use an underscore to separate the
            # array job id and the array task id, this isn't guaranteed to be universal
            # (e.g. SGE will use a period instead, xyz.abc).
            array_distributor_ids.append(tuple(distributor_id_split))
        elif len(distributor_id_split) == 1:
            # Add to non array queue
            nonarray_distributor_ids.append(ti.distributor_id)
        else:
            logger.warning(f"Couldn't parse distributor ID {ti.distributor_id}")
            # Don't repopulate the queue since we can't do anything with this task instance

        # Add the task instance to the distributor ID -> task instance ID map
        dict_dist_ti[str(ti.distributor_id)] = ti

    # get job_step data
    # Case is needed since we want to return a concatenation of parent array job and subtask
    # id as the job_id for array jobs.
    sql_step = "SELECT " \
               "CASE " \
               "    WHEN job.id_array_job = 0 THEN job.id_job " \
               "    ELSE CONCAT(job.id_array_job, '_', job.id_array_task) " \
               "END AS job_id, " \
               "job.time_end - job.time_start AS elapsed, " \
               "job.tres_alloc, step.tres_usage_in_max " \
               "FROM general_step_table step " \
               "INNER JOIN general_job_table job ON step.job_db_inx = job.job_db_inx " \
               "WHERE step.deleted = 0 "

    # Issue two separate queries for array and non-array jobs. The where clauses are
    # constructed differently, and figured this is simpler than a complex CASE statement.
    #
    non_array_clause = "AND job.id_job IN :job_ids"
    array_clause = "AND (job.id_array_job, job.id_array_task) IN :array_id_tuples"

    # Initialize results as empty lists. If we have anything to query on we will
    array_steps, non_array_steps = [], []

    if nonarray_distributor_ids:
        non_array_steps = session.execute(
            sql_step + non_array_clause, {"job_ids": nonarray_distributor_ids}
        ).all()

    if array_distributor_ids:
        array_steps = session.execute(
            sql_step + array_clause, {"array_id_tuples": array_distributor_ids}
        ).all()

    session.commit()

    for step in non_array_steps + array_steps:
        if step[0] in raw_usage_stats:
            job_stats = raw_usage_stats[step[0]]
            job_stats["runtime"] = max(job_stats["runtime"], step[1])
        else:
            job_stats = {"runtime": step[1], "mem": 0}
        tres_alloc = ast.literal_eval("{" + step[2].replace("=", ":") + "}")
        for tres_type_name in ["cpu", "node", "billing"]:
            job_stats[tres_type_name] = tres_alloc[tres_types[tres_type_name]]
        tres_usage_in_max = ast.literal_eval("{" + step[3].replace("=", ":") + "}")
        job_stats["mem"] += tres_usage_in_max.get(tres_types["mem"], 0)
        raw_usage_stats[step[0]] = job_stats

    for k, v in raw_usage_stats.items():
        v["usage_str"] = v.copy()
        v["wallclock"] = v.pop("runtime")
        v["maxrss"] = v.pop("mem")
        logger.info(f"{k}: {v}")
        all_usage_stats[dict_dist_ti[str(k)]] = v
    return all_usage_stats


def _get_config(config: UsageConfig = None) -> dict:
    if config is None:
        config = UsageConfig.from_defaults()
    return {
        "conn_str": config.conn_str,
        "conn_slurm_sdb_str": config.conn_slurm_sdb_str,
        "polling_interval": config.slurm_polling_interval,
        "max_update_per_sec": config.slurm_max_update_per_second,
    }


def q_forever(init_time: datetime.datetime = datetime.datetime(2022, 4, 8),
              integrator_config: UsageConfig = None, never_retire: bool = True) -> None:
    """A never stop method running in a thread that queries the SLURM and Jobmon databases.

    It constantly queries the maxrss value from the SLURM accounting database
    for completed jobmon jobs. If the maxrss is not found in the database,
    put the execution id back to the queue.

    The default initialization time is set to 4/8/2022 since that's when Infra began
    replicating the production accounting databases. We cannot query data from before that
    date.
    """
    # We enforce Pacific time since that's what the database uses.
    # Careful, this will set a global environment variable on initializing the q_forever loop.
    os.environ['TZ'] = 'America/Los_Angeles'
    time.tzset()

    # allow the service to decide the time to go back to fill maxrss/maxpss
    last_heartbeat = init_time
    integrator = UsageIntegrator(integrator_config)

    # only query each ti once in one polling_interval
    initial_q_size = UsageQ.get_size()
    processed_size = 0
    while UsageQ.keep_running:
        # Since there isn't a good way to specify the thread priority in Python,
        # put a sleep in each attempt to not overload the CPU.
        # The avg daily job instance is about 20k; thus, sleep(1) should be ok.
        time.sleep(1)
        # if all this in Q has already been integrated in this polling_interval, skip
        if processed_size < initial_q_size:
            logger.info(f"Processed size in this polling interval: {processed_size}")
            # Update slurm_max_update_per_second of jobs as defined in jobmon.cfg
            task_instances = [
                UsageQ.get() for _ in range(integrator.config["max_update_per_sec"])
            ]
            # If the queue is empty, drop the None entries
            task_instances = [t for t in task_instances if t is not None]
            processed_size += len(task_instances)
            integrator.update_resources_in_db(task_instances)

        # Query DB to add newly completed jobs to q and log q length
        current_time = datetime.datetime.now()
        if current_time - last_heartbeat > \
                datetime.timedelta(seconds=integrator.config["polling_interval"]):
            logger.info("UsageQ length: {}, last heartbeat time: {}".format(
                UsageQ.get_size(), str(last_heartbeat)
            ))
            try:
                integrator.populate_queue(last_heartbeat)
                # restart counter
                initial_q_size = UsageQ.get_size()
                processed_size = 0
                logger.debug(f"Q length: {UsageQ.get_size()}")
            except Exception as e:
                logger.error(str(e))
            finally:
                last_heartbeat = current_time
    integrator.session.close()
