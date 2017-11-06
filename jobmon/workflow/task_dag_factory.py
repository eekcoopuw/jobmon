from datetime import datetime
import logging
from getpass import getuser

from jobmon.config import config
from jobmon.job_instance_factory import execute_sge
from jobmon.job_list_manager import JobListManager
from jobmon.requester import Requester
from jobmon.workflow.task_dag import TaskDag

logger = logging.getLogger(__name__)


class TaskDagFactory(object):
    """
    Factory class for TaskDags.

    This creates its own job_list_manager.
    """

    def __init__(self):
        logger.debug("TaskDagFactory created")

    def create_task_dag(self, name=None):
        """
        Creates a new DAG, complete with its own JobListManager

        Returns:
             the new task dag
        """
        logger.debug("DagFactory creating new DAG {}".format(name))
        req = Requester(config.jm_rep_conn)
        rc, dag_id = req.send_request({
            'action': 'add_task_dag',
            'kwargs': {'name': name, 'user': getuser()}
        })
        job_list_manager = JobListManager(dag_id, executor=execute_sge, start_daemons=True)
        dag = TaskDag(dag_id=dag_id, name=name, job_list_manager=job_list_manager, created_date=datetime.utcnow())
        logger.debug("New TaskDag created {}".format(dag))
        return dag
