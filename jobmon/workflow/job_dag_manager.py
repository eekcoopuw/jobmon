from datetime import datetime
import logging
from getpass import getuser

from jobmon.config import config
from jobmon.job_instance_factory import execute_sge
from jobmon.job_list_manager import JobListManager
from jobmon.job_query_server import JobQueryServer
from jobmon.job_state_manager import JobStateManager
from jobmon.requester import Requester
from jobmon.workflow.job_dag import JobDag

logger = logging.getLogger(__name__)


class JobDagManager(object):
    """
    Factory class for JobDags.

    This creates its own job_list_manager.
    """

    def __init__(self):
        logger.debug("JobDagManager created")

    def create_job_dag(self, name=None):
        """
        Creates a new DAG, complete with its own JobListManager
        
        Returns:
             the new job dag
        """
        logger.debug("JobDagManager creating new DAG {}".format(name))
        req = Requester(config.jm_rep_conn)
        rc, dag_id = req.send_request({
            'action': 'add_job_dag',
            'kwargs': {'name': name, 'user': getuser()}
        })
        job_list_manager = JobListManager(dag_id, executor=execute_sge, start_daemons=True)
        dag = JobDag(dag_id=dag_id, name=name, job_list_manager=job_list_manager,
                     job_state_manager=JobStateManager.it(), job_query_server=JobQueryServer.it(),
                     created_date=datetime.utcnow())
        logger.debug("New JobDag created {}".format(dag))
        return dag

