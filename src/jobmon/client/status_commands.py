import getpass
from typing import List, Tuple, Optional

import structlog as logging
import pandas as pd

from jobmon.client.client_config import ClientConfig
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


def workflow_status(workflow_id: List[int] = [], user: List[str] = [],
                    json: bool = False, requester_url: Optional[str] = None) -> pd.DataFrame:
    """Get metadata about workflow progress

    Args:
        workflow_id: workflow_id/s to retrieve info for. If not specified will pull all
            workflows by user
        user: user/s to retrieve info for. If not specified will return for current user.
        json: Flag to return data as JSON

    Returns:
        dataframe of all workflows and their status
    """
    logger.debug("workflow_status workflow_id:{}".format(str(workflow_id)))
    msg: dict = {}
    if workflow_id:
        msg["workflow_id"] = workflow_id
    if user:
        msg["user"] = user
    else:
        msg["user"] = getpass.getuser()

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    rc, res = requester.send_request(
        app_route="/viz/workflow_status",
        message=msg,
        request_type="get",
        logger=logger
    )
    if json:
        return res["workflows"]
    else:
        return pd.read_json(res["workflows"])


def workflow_tasks(workflow_id: int, status: List[str] = None, json: bool = False,
                   requester_url: Optional[str] = None) -> pd.DataFrame:
    """Get metadata about task state for a given workflow

    Args:
        workflow_id: workflow_id/s to retrieve info for
        status: limit task state to one of [PENDING, RUNNING, DONE, FATAL] tasks
        json: Flag to return data as JSON

    Returns:
        Dataframe of tasks for a given workflow
    """
    logger.info("workflow id: {}".format(workflow_id))
    msg = {}
    if status:
        msg["status"] = [i.upper() for i in status]

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    rc, res = requester.send_request(
        app_route=f"/viz/workflow/{workflow_id}/workflow_tasks",
        message=msg,
        request_type="get",
        logger=logger
    )
    if json:
        return res["workflow_tasks"]
    else:
        return pd.read_json(res["workflow_tasks"])


def task_status(task_ids: List[int], status: Optional[List[str]] = None, json: bool = False,
                requester_url: Optional[str] = None) -> Tuple[str, pd.DataFrame]:
    """Get metadata about a task and its task instances

    Args:
        task_ids: a list of task_ids to retrieve task_instance metadata for
        status: a list of statuses to check for
        json: Flag to return data as JSON

    Returns:
        Task status and task_instance metadata
    """
    logger.info("task_status task_ids:{}".format(str(task_ids)))
    msg = {}
    msg["task_ids"] = task_ids
    if status:
        msg["status"] = [i.upper() for i in status]

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    rc, res = requester.send_request(
        app_route="/viz/task_status",
        message=msg,
        request_type="get",
        logger=logger
    )
    if json:
        return res["task_instance_status"]
    else:
        return pd.read_json(res["task_instance_status"])


def rate_limit(workflow_id: int, max_tasks: int, requester_url: Optional[str] = None) -> str:
    """ Update a workflow's max_running_instances field in the database

    Used to dynamically adjust the allowed number of jobs concurrently running.

    Args:
        workflow_id: ID of the running workflow whose max_running value needs to be reset
        max_tasks: new allowed value of parallel tasks

    Returns: string displaying success or failure of the update.
    """

    msg = {}
    msg["max_tasks"] = max_tasks

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    _, resp = requester.send_request(
        app_route=f"/viz/workflow/{workflow_id}/update_max_running",
        message=msg,
        request_type="put")

    return resp['message']
