import getpass
from typing import List, Tuple, Optional

import structlog as logging
import pandas as pd

from jobmon.client.client_config import ClientConfig
from jobmon.requester import Requester
from jobmon.constants import TaskStatus


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
        app_route="/cli/workflow_status",
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
        app_route=f"/cli/workflow/{workflow_id}/workflow_tasks",
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
        app_route="/cli/task_status",
        message=msg,
        request_type="get",
        logger=logger
    )
    if json:
        return res["task_instance_status"]
    else:
        return pd.read_json(res["task_instance_status"])


def concurrency_limit(workflow_id: int, max_tasks: int, requester_url: Optional[str] = None) -> str:
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
        app_route=f"/cli/workflow/{workflow_id}/update_max_running",
        message=msg,
        request_type="put")

    return resp['message']


def update_task_status(task_ids: List[int], workflow_id: int, new_status: str,
                       requester_url: Optional[str] = None) -> None:
    """
    Set the specified task IDs to the new status, pending validation.

    Args:
        task_ids: List of task IDs to reset in the database
        workflow_id: The workflow to which each task belongs. Users can only self-service
            1 workflow at a time for the moment.
        new_status: the status to set tasks to
    """

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    # Validate the username is appropriate
    user = getpass.getuser()

    validate_username(workflow_id, user, requester)
    workflow_status = validate_workflow(task_ids, requester)

    # Validate the allowed statuses. For now, only "D" and "G" allowed.
    allowed_statuses = [TaskStatus.REGISTERED, TaskStatus.DONE]
    assert new_status in allowed_statuses, f"Only {allowed_statuses} allowed to be set via CLI"

    # Conditional logic: If the new status is "D", only need to set task to "D"
    # Else: All downstreams must also be set to "G", and task instances set to "K"
    if new_status == TaskStatus.REGISTERED:
        subdag_tasks = get_sub_task_tree(task_ids).keys()
        task_ids = task_ids + [*subdag_tasks]

    _, resp = requester.send_request(
        app_route='/cli/task/update_statuses',
        message={'task_ids': task_ids,
                 'new_status': new_status,
                 'workflow_status': workflow_status,
                 'workflow_id': workflow_id},
        request_type='put')

    return resp


def validate_username(workflow_id: int, username: str, requester: Requester) -> None:

    # Validate that the user is approved to make these changes
    rc, res = requester.send_request(
        app_route=f"/cli/workflow/{workflow_id}/usernames",
        message={},
        request_type="get")

    if username not in res['usernames']:
        raise AssertionError(f"User {username} is not allowed to reset this workflow.",
                             f"Only the following users have permission: {', '.join(res['usernames'])}")

    return


def validate_workflow(task_ids: List[int], requester: Requester) -> None:
    rc, res = requester.send_request(
        app_route="/cli/workflow_validation",
        message={'task_ids': task_ids},
        request_type="get")

    if not bool(res["validation"]):
        raise AssertionError("The give task ids belong to multiple workflow.")
    return res['workflow_status']


def get_sub_task_tree(task_ids: list, task_status: list = None, requester: Requester = None) -> dict:
    # This is to make the test case happy. Otherwise, requester should not be None.
    if requester is None:
        requester = Requester(ClientConfig.from_defaults().url)
    # Valid input
    rc, res = requester.send_request(
        app_route=f"/cli/task/subdag",
        message={'task_ids': task_ids,
            'task_status': task_status},
        request_type="get")
    if rc != 200:
        raise AssertionError(f"Server return HTTP error code: {rc}")
    task_tree_dict = res["sub_task"]
    return task_tree_dict
