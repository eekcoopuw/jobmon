from datetime import datetime
import logging
from getpass import getuser

from jobmon.client.the_client_config import get_the_client_config
from jobmon.client.requester import Requester
from jobmon.models.task_dag import TaskDagMeta

logger = logging.getLogger(__name__)


class TaskDagMetaFactory(object):
    """
    Factory class for TaskDags.

    This creates its own job_list_manager.
    """

    def __init__(self):
        logger.debug("TaskDagFactory created")

    def create_task_dag(self, name, dag_hash, user):
        """
        Creates a new DAG, complete with its own JobListManager

        Returns:
             the new task dag
        """
        logger.debug("DagFactory creating new DAG {}".format(name))
        req = Requester(get_the_client_config(), 'jsm')
        rc, response = req.send_request(
            app_route='/add_task_dag',
            message={'name': name, 'user': getuser(), 'dag_hash': dag_hash,
                     'created_date': str(datetime.utcnow())},
            request_type='post')
        tdm = TaskDagMeta(dag_id=response['dag_id'], name=name,
                          created_date=datetime.utcnow())
        logger.debug("New TaskDag created {}".format(tdm))
        return tdm
