"""Commands to check for workflow and task status (from CLI)."""
import getpass
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from jobmon.client.client_config import ClientConfig
from jobmon.constants import TaskStatus, WorkflowStatus
from jobmon.requester import Requester
from jobmon.serializers import SerializeTaskTemplateResourceUsage


logger = logging.getLogger(__name__)


def workflow_status(
    workflow_id: List[int] = None,
    user: List[str] = None,
    json: bool = False,
    requester_url: Optional[str] = None,
    limit: Optional[int] = 5,
) -> pd.DataFrame:
    """Get metadata about workflow progress.

    Args:
        workflow_id: workflow_id/s to retrieve info for. If not specified will pull all
            workflows by user
        user: user/s to retrieve info for. If not specified will return for current user.
        limit: return # of records order by wf id desc. Return 5 if not provided;
            return all if [], [<0].
        json: Flag to return data as JSON
        requester_url (str): url to communicate with the flask services

    Returns:
        dataframe of all workflows and their status
    """
    if workflow_id is None:
        workflow_id = []
    if user is None:
        user = []
    logger.debug("workflow_status workflow_id:{}".format(str(workflow_id)))
    msg: dict = {}
    if workflow_id:
        msg["workflow_id"] = workflow_id
    if user:
        msg["user"] = user
    else:
        msg["user"] = getpass.getuser()
    msg["limit"] = limit

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    rc, res = requester.send_request(
        app_route="/workflow_status", message=msg, request_type="get", logger=logger
    )
    if json:
        return res["workflows"]
    else:
        return pd.read_json(res["workflows"])


def workflow_tasks(
    workflow_id: int,
    status: List[str] = None,
    json: bool = False,
    requester_url: Optional[str] = None,
    limit: int = 5,
) -> pd.DataFrame:
    """Get metadata about task state for a given workflow.

    Args:
        workflow_id: workflow_id/s to retrieve info for
        status: limit task state to one of [PENDING, RUNNING, DONE, FATAL] tasks
        json: Flag to return data as JSON
        requester_url (str): url to communicate with the flask services
        limit: return # of records order by wf id desc. Return 5 if not provided

    Returns:
        Dataframe of tasks for a given workflow
    """
    logger.debug("workflow id: {}".format(workflow_id))
    msg: Dict[str, Union[List[str], int]] = {}
    if status:
        msg["status"] = [i.upper() for i in status]
    msg["limit"] = limit

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/workflow_tasks",
        message=msg,
        request_type="get",
        logger=logger,
    )
    if json:
        return res["workflow_tasks"]
    else:
        return pd.read_json(res["workflow_tasks"])


def task_template_resources(
    task_template_version: int,
    workflows: Optional[list] = None,
    node_args: Optional[Dict] = None,
    requester_url: Optional[str] = None,
    ci: Optional[float] = None,
) -> Tuple:
    """Get aggregate resource usage data for a given TaskTemplateVersion.

    Args:
        task_template_version: The task template version ID the user wants to find the
            resource usage of.
        workflows: list of workflows a user wants query by.
        node_args: dictionary of node arguments a user wants to query by.
        requester_url: url to communicate with the flask services.
        ci: confidence interval. Not calculate if None.

    Returns:
        Dataframe of TaskTemplate resource usage
    """
    message = {"task_template_version_id": task_template_version}
    if workflows:
        message["workflows"] = workflows
    if node_args:
        message["node_args"] = node_args
    if ci:
        message["ci"] = ci

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    app_route = "/task_template_resource_usage"
    return_code, response = requester.send_request(
        app_route=app_route, message=message, request_type="post", logger=logger
    )

    def format_bytes(value):
        if value is not None:
            return str(value) + "B"
        else:
            return value

    kwargs = SerializeTaskTemplateResourceUsage.kwargs_from_wire(response)
    resources = {
        "num_tasks": kwargs["num_tasks"],
        "min_mem": format_bytes(kwargs["min_mem"]),
        "max_mem": format_bytes(kwargs["max_mem"]),
        "mean_mem": format_bytes(kwargs["mean_mem"]),
        "min_runtime": kwargs["min_runtime"],
        "max_runtime": kwargs["max_runtime"],
        "mean_runtime": kwargs["mean_runtime"],
        "median_mem": format_bytes(kwargs["median_mem"]),
        "median_runtime": kwargs["median_runtime"],
        "ci_mem": kwargs["ci_mem"],
        "ci_runtime": kwargs["ci_runtime"],
    }

    return resources


def task_status(
    task_ids: List[int],
    status: Optional[List[str]] = None,
    json: bool = False,
    requester_url: Optional[str] = None,
) -> Tuple[str, pd.DataFrame]:
    """Get metadata about a task and its task instances.

    Args:
        task_ids: a list of task_ids to retrieve task_instance metadata for.
        status: a list of statuses to check for.
        json: Flag to return data as JSON.
        requester_url: url to communicate with the Flask service.

    Returns:
        Task status and task_instance metadata
    """
    logger.debug("task_status task_ids:{}".format(str(task_ids)))
    msg: Dict[str, Union[List[str], List[int]]] = {"task_ids": task_ids}
    if status:
        msg["status"] = [i.upper() for i in status]

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    rc, res = requester.send_request(
        app_route="/task_status", message=msg, request_type="get", logger=logger
    )
    if json:
        return res["task_instance_status"]
    else:
        return pd.read_json(res["task_instance_status"])


def concurrency_limit(
    workflow_id: int, max_tasks: int, requester_url: Optional[str] = None
) -> str:
    """Update a workflow's max_running_instances field in the database.

    Used to dynamically adjust the allowed number of jobs concurrently running.

    Args:
        workflow_id (int): ID of the running workflow whose max_running value needs to be reset
        max_tasks (int) : new allowed value of parallel tasks
        requester_url (str): url to requester to connect to Flask service.

    Returns: string displaying success or failure of the update.
    """
    msg = {}
    msg["max_tasks"] = max_tasks

    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    _, resp = requester.send_request(
        app_route=f"/workflow/{workflow_id}/update_max_running",
        message=msg,
        request_type="put",
    )

    return resp["message"]


def _chunk_ids(ids: List[int], chunk_size=100) -> List[List[int]]:
    """Chunk the ids into a list of 100 ids list

    Args:
        ids: list of ids
        chunk_size: the size of each chunk; default to 100

    Returns: a list of list

    """
    return_list = []
    return_list.append(ids[0 : min(chunk_size, len(ids))])
    i = 1
    while i * chunk_size < len(ids):
        return_list.append(ids[i * chunk_size : min((i + 1) * chunk_size, len(ids))])
        i += 1
    return return_list


def update_task_status(
    task_ids: List[int],
    workflow_id: int,
    new_status: str,
    force: bool = False,
    recursive: bool = False,
    requester_url: Optional[str] = None,
) -> None:
    """Set the specified task IDs to the new status, pending validation.

    Args:
        task_ids: List of task IDs to reset in the database
        workflow_id: The workflow to which each task belongs. Users can only self-service
            1 workflow at a time for the moment.
        new_status: the status to set tasks to
        force: if true, allow all source statuses and all workflow statuses.
        recursive: if true and force, apply recursive update_status downstream
            or upstream depending on new_status
            (upstream if new_status == 'D'; downstream if new_status == 'G').
    """
    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    # Validate the username is appropriate
    user = getpass.getuser()

    validate_username(workflow_id, user, requester)
    workflow_status = validate_workflow(task_ids, requester, force)

    # Validate the allowed statuses. For now, only "D" and "G" allowed.
    allowed_statuses = [TaskStatus.REGISTERED, TaskStatus.DONE]
    assert (
        new_status in allowed_statuses
    ), f"Only {allowed_statuses} allowed to be set via CLI"

    # Conditional logic: If the new status is "D", only need to set task to "D"
    # Else: All downstreams must also be set to "G", and task instances set to "K"
    if force and recursive:
        rc, res = requester.send_request(
            app_route="/tasks_recursive/" + ("up" if new_status == "D" else "down"),
            message={"task_ids": task_ids},
            request_type="put",
            logger=logger,
        )
        if rc != 200:
            raise AssertionError(f"Server return HTTP error code: {rc}")
        task_ids = res["task_ids"]
    else:
        if new_status == TaskStatus.REGISTERED:
            subdag_tasks = get_sub_task_tree(task_ids).keys()
            task_ids = task_ids + [*subdag_tasks]

    # We want to prevent excessive requests, with a hard-limit of 10,000 set up
    # to avoid churning on the server.
    if len(task_ids) > 10_000:
        raise AssertionError(
            f"There are too many tasks ({len(task_ids)}) requested "
            f"for the update. Request denied."
        )

    task_ids_chunked = _chunk_ids(task_ids)
    for chunk in task_ids_chunked:
        _, resp = requester.send_request(
            app_route="/task/update_statuses",
            message={
                "task_ids": chunk,
                "new_status": new_status,
                "workflow_status": workflow_status,
                "workflow_id": workflow_id,
            },
            request_type="put",
        )

    return resp


def update_config(cc: ClientConfig) -> None:
    """Update .jobmon.ini.

    Args:
        cc: new ClientConfig
    """
    import os
    from jobmon.config import INSTALLED_CONFIG_FILE
    import configparser

    if os.path.isfile(INSTALLED_CONFIG_FILE):
        edit = configparser.ConfigParser()
        edit.read(INSTALLED_CONFIG_FILE)
        client = edit["client"]
        if (
            client["web_service_fqdn"] == cc.host
            and client["web_service_port"] == cc.port
        ):
            print(
                "The new values are the same as in the config file. "
                "No update is made to the config file."
            )
            return
        else:
            client["web_service_fqdn"] = cc.host
            client["web_service_port"] = str(cc.port)
            with open(INSTALLED_CONFIG_FILE, "w") as configfile:
                edit.write(configfile)
    else:
        config = configparser.ConfigParser()
        config.add_section("client")
        config.set("client", "web_service_fqdn", cc.host)
        config.set("client", "web_service_port", str(cc.port))
        with open(INSTALLED_CONFIG_FILE, "w") as configfile:
            config.write(configfile)

    print(
        f"Config file {INSTALLED_CONFIG_FILE} has been updated",
        f"with new web_service_fqdn = {cc.host} web_service_port = {cc.port}.",
    )

    return


def validate_username(workflow_id: int, username: str, requester: Requester) -> None:
    """Validate that the user is approved to make these changes."""
    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/validate_username/{username}",
        message={},
        request_type="get",
        logger=logger,
    )
    if not res["validation"]:
        raise AssertionError(f"User {username} is not allowed to reset this workflow.")
    return


def validate_workflow(
    task_ids: List[int], requester: Requester, force: bool = False
) -> WorkflowStatus:
    """
    Validate that the task_ids provided belong to the expected workflow,
    and the workflow status is in expected status unless we want to force
    it through.
    """
    rc, res = requester.send_request(
        app_route="/workflow_validation",
        message={"task_ids": task_ids, "force": "true" if force else "false"},
        request_type="post",
    )

    if not bool(res["validation"]):
        raise AssertionError(
            "The workflow status of the given task ids are out of "
            "scope of the following required statuses "
            "(FAILED, DONE, ABORTED, HALTED) or multiple workflow statuses "
            "were found."
        )
    return res["workflow_status"]


def get_sub_task_tree(
    task_ids: list, task_status: list = None, requester: Requester = None
) -> dict:
    """Get the sub_tree from tasks to ensure that they end up in the right states."""
    # This is to make the test case happy. Otherwise, requester should not be None.
    if requester is None:
        requester = Requester(ClientConfig.from_defaults().url)
    # Valid input
    rc, res = requester.send_request(
        app_route="/task/subdag",
        message={"task_ids": task_ids, "task_status": task_status},
        request_type="post",
    )
    if rc != 200:
        raise AssertionError(f"Server return HTTP error code: {rc}")
    task_tree_dict = res["sub_task"]
    return task_tree_dict


def get_task_dependencies(task_id: int, requester_url: Optional[str] = None) -> dict:
    """Get the upstream and down stream of a task"""
    # This is to make the test case happy. Otherwise, requester should not be None.
    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)
    # Valid input
    rc, res = requester.send_request(
        app_route=f"/task_dependencies/{task_id}", message={}, request_type="get"
    )
    if rc != 200:
        if rc == 404:
            raise AssertionError(
                f"Server return HTTP error code: {rc}. "
                f"The jobmon server version may not support this command."
            )
        else:
            raise AssertionError(f"Server return HTTP error code: {rc}")
    return res


def workflow_reset(workflow_id: int, requester: Requester) -> str:
    """Workflow reset.
    Return:
        A string to indicate the workflow_reset result.
    Args:
        workflow_id: the workflow id to be reset.
    """
    username = getpass.getuser()

    rc, res = requester.send_request(
        app_route=f"/workflow/{workflow_id}/validate_for_workflow_reset/{username}",
        message={},
        request_type="get",
        logger=logger,
    )
    if rc != 200:
        raise AssertionError(f"Server return HTTP error code: {rc}")
    if res["workflow_run_id"]:
        rc, _ = requester.send_request(
            app_route=f"/workflow/{workflow_id}/reset",
            message={},
            request_type="put",
            logger=logger,
        )
        if rc != 200:
            raise AssertionError(f"Server return HTTP error code: {rc}")
        wr_return = f"Workflow {workflow_id} has been reset."
    else:
        wr_return = (
            f"User {username} is not the latest initiator of "
            f"workflow {workflow_id} that has resulted in error('E' status). "
            f"The workflow {workflow_id} has not been reset."
        )

    return wr_return


def _get_yaml_data(
    wfid: int, tid: int, v_mem: str, v_core: str, v_runtime: str, requester: Requester
):

    key_map_m = {"avg": "mean_mem", "min": "min_mem", "max": "max_mem"}
    key_map_r = {"avg": "mean_runtime", "min": "min_runtime", "max": "max_runtime"}

    # Get task template version ids
    rc, res = requester.send_request(
        app_route="/get_task_template_version",
        message={"task_id": tid} if wfid is None else {"workflow_id": wfid},
        request_type="get",
        logger=logger,
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for get_task_template_version."
        )
    ttvis_dic = dict()
    # data structure: {ttv_id: [name, core, mem, runtime, queue]}
    for t in res["task_template_version_ids"]:
        ttvis_dic[t["id"]] = [t["name"]]

    # get core
    ttvis = str([i for i in ttvis_dic.keys()]).replace("[", "(").replace("]", ")")
    rc, res = requester.send_request(
        app_route="/get_requested_cores",
        message={"task_template_version_ids": f"{ttvis}"},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for /get_requested_cores."
        )
    core_info = res["core_info"]
    for record in core_info:
        ttvis_dic[int(record["id"])].append(record[v_core])

    # Get actually mem and runtime for each ttvi
    for ttv in ttvis_dic.keys():
        rc, res = requester.send_request(
            app_route="/task_template_resource_usage",
            message={"task_template_version_id": ttv},
            request_type="post",
        )
        if rc != 200:
            raise AssertionError(
                f"Server returns HTTP error code: {rc} "
                f"for /task_template_resource_usage."
            )
        usage = SerializeTaskTemplateResourceUsage.kwargs_from_wire(res)
        ttvis_dic[int(ttv)].append(int(usage[key_map_m[v_mem]]))
        ttvis_dic[int(ttv)].append(int(usage[key_map_r[v_runtime]]))

    # get queue
    rc, res = requester.send_request(
        app_route="/get_most_popular_queue",
        message={"task_template_version_ids": f"{ttvis}"},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for /get_most_popular_queue."
        )
    for record in res["queue_info"]:
        ttvis_dic[int(record["id"])].append(record["queue"])
    return ttvis_dic


def _create_yaml(data: Dict = None, clusters: List = []):
    yaml = "task_template_resources:\n"
    if data is None or clusters is None or len(clusters) == 0:
        return yaml
    for ttv in data.keys():
        yaml += f"  {data[ttv][0]}:\n"  # name
        for cluster in clusters:
            yaml += f"    {cluster}:\n"  # cluster
            yaml += f"      num_cores: {data[ttv][1]}\n"  # core
            yaml += f'      m_mem_free: "{data[ttv][2]}B"\n'  # mem
            yaml += f"      max_runtime_seconds: {data[ttv][3]}\n"  # runtime
            yaml += f'      queue: "{data[ttv][4]}"\n'  # queue
    return yaml


def create_resource_yaml(
    wfid: int,
    tid: int,
    v_mem: str,
    v_core: str,
    v_runtime: str,
    clusters: List,
    requester_url: Optional[str] = None,
) -> str:
    """The method to create resource yaml."""
    if requester_url is None:
        requester_url = ClientConfig.from_defaults().url
    requester = Requester(requester_url)

    ttvis_dic = _get_yaml_data(wfid, tid, v_mem, v_core, v_runtime, requester)
    yaml = _create_yaml(ttvis_dic, clusters)
    return yaml
