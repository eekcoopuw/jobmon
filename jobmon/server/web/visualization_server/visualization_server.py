from http import HTTPStatus as StatusCodes
from flask import jsonify, request, Blueprint, current_app as app
from werkzeug.local import LocalProxy


import pandas as pd


from jobmon.models import DB
from jobmon.constants import WorkflowStatus as Statuses


jvs = Blueprint("visualization_server", __name__)


logger = LocalProxy(lambda: app.logger)


_viz_label_mapping = {
    "A": "PENDING",
    "G": "PENDING",
    "Q": "PENDING",
    "I": "PENDING",
    "E": "PENDING",
    "R": "RUNNING",
    "F": "FATAL",
    "D": "DONE"
}
_reversed_viz_label_mapping = {
    "PENDING": ["A", "G", "Q", "I", "E"],
    "RUNNING": ["R"],
    "FATAL": ["F"],
    "DONE": ["D"]
}
_task_instance_label_mapping = {
    "B": "PENDING",
    "I": "PENDING",
    "R": "RUNNING",
    "E": "FATAL",
    "Z": "FATAL",
    "W": "FATAL",
    "U": "FATAL",
    "K": "FATAL",
    "D": "DONE"
}
_reversed_task_instance_label_mapping = {
    "PENDING": ["B", "I"],
    "RUNNING": ["R"],
    "FATAL": ["E", "Z", "W", "U", "K"],
    "DONE": ["D"]
}
_viz_order = ["PENDING", "RUNNING", "DONE", "FATAL"]


@jvs.route("/health", methods=['GET'])
def health():
    """
    Test connectivity to the database, return 200 if everything is ok
    Defined in each module with a different route, so it can be checked individually
    """
    time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS time").fetchone()
    time = time['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    DB.session.commit()
    # Assume that if we got this far without throwing an exception, we should be online
    resp = jsonify(status='OK')
    resp.status_code = StatusCodes.OK
    return resp


@jvs.route("/workflow_validation", methods=['GET'])
def get_workflow_validation_status():
    # initial params
    task_ids = request.args.getlist('task_ids')

    # if the given list is empty, return True
    if len(task_ids) == 0:
        resp = jsonify(validation=True)
        resp.status_code = StatusCodes.OK
        return resp

    task_list = ''
    for id in task_ids:
        task_list = task_list + str(id) + ","
    task_list = task_list[:-1]

    # execute query
    q = f"""
        SELECT
            distinct workflow_id, status
        FROM task
        WHERE id IN ({task_list})
    """
    res = DB.session.execute(q).fetchall()
    # Validate if all tasks are in the same workflow and the workflow status is dead
    if len(res) == 1 and res[0][1] in (Statuses.FAILED, Statuses.DONE, Statuses.ABORTED, Statuses.SUSPENDED):
        resp = jsonify(validation=True)
    else:
        resp = jsonify(validation=False)

    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow_status', methods=['GET'])
def get_workflow_status():
    # initial params
    params = {}
    user_request = request.args.getlist('user')
    if user_request == "all":  # specifying all is equivalent to None
        user_request = []
    workflow_request = request.args.getlist('workflow_id')
    if workflow_request == "all":  # specifying all is equivalent to None
        workflow_request = []

    where_clause = ""
    # convert workflow request into sql filter
    if workflow_request:
        workflow_request = [int(w) for w in workflow_request]
        params["workflow_id"] = workflow_request
        where_clause = "WHERE workflow.id in :workflow_id "
    else:  # if we don't specify workflow then we use the users
        # convert user request into sql filter
        if user_request:
            params["user"] = user_request
            where_clause = "WHERE workflow_run.user in :user "

    # execute query
    q = """
        SELECT
            workflow.id as WF_ID,
            workflow.name as WF_NAME,
            workflow_status.label as WF_STATUS,
            count(task.status) as TASKS,
            task.status AS STATUS,
            sum(
                CASE
                    WHEN num_attempts <= 1 THEN 0
                    ELSE num_attempts - 1
                END
            ) as RETRIES
        FROM workflow
        JOIN workflow_run
            ON workflow.id = workflow_run.workflow_id
        JOIN task
            ON workflow.id = task.workflow_id
        JOIN workflow_status
            ON workflow_status.id = workflow.status
        {where_clause}
        GROUP BY workflow.id, task.status, workflow.name, workflow_status.label
    """.format(where_clause=where_clause)
    res = DB.session.execute(q, params).fetchall()

    if res:

        # assign to dataframe for aggregation
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to viz statuses
        df.STATUS.replace(to_replace=_viz_label_mapping, inplace=True)

        # aggregate totals by workflow and status
        df = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS", "STATUS"]
                        ).agg({'TASKS': 'sum', 'RETRIES': 'sum'})

        # pivot wide by task status
        tasks = df.pivot_table(
            values="TASKS",
            index=["WF_ID", "WF_NAME", "WF_STATUS"],
            columns="STATUS",
            fill_value=0)
        for col in _viz_order:
            if col not in tasks.columns:
                tasks[col] = 0
        tasks = tasks[_viz_order]

        # aggregate again without status to get the totals by workflow
        retries = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS"]
                             ).agg({'TASKS': 'sum', 'RETRIES': 'sum'})

        # combine datasets
        df = pd.concat([tasks, retries], axis=1)

        # compute pcts and format
        for col in _viz_order:
            df[col + "_pct"] = (
                df[col].astype(float) / df["TASKS"].astype(float)) * 100
            df[col + "_pct"] = df[[col + "_pct"]].round(1)
            df[col] = (
                df[col].astype(int).astype(str) +
                " (" + df[col + "_pct"].astype(str) + "%)")

        # df.replace(to_replace={"0 (0.0%)": "NA"}, inplace=True)
        # final order
        df = df[["TASKS"] + _viz_order + ["RETRIES"]]
        df = df.reset_index()
        df = df.to_json()
        resp = jsonify(workflows=df)
    else:
        df = pd.DataFrame({},
                          columns=["WF_ID", "WF_NAME", "WF_STATUS", "TASKS",
                                   "PENDING", "RUNNING", "DONE", "FATAL",
                                   "RETRIES"]).to_json()
        resp = jsonify(workflows=df)

    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow/<workflow_id>/workflow_tasks', methods=['GET'])
def get_workflow_tasks(workflow_id):
    params = {"workflow_id": workflow_id}
    where_clause = "WHERE workflow.id = :workflow_id"
    status_request = request.args.getlist('status', None)

    if status_request:
        params["status"] = [i for arg in status_request
                            for i in _reversed_viz_label_mapping[arg]]
        where_clause += " AND task.status in :status"

    q = """
        SELECT
            task.id AS TASK_ID,
            task.name AS TASK_NAME,
            task.status AS STATUS,
            CASE
                WHEN num_attempts <= 1 THEN 0
                ELSE num_attempts - 1
            END AS RETRIES
        FROM workflow
        JOIN task
            ON workflow.id = task.workflow_id
        {where_clause}""".format(where_clause=where_clause)
    res = DB.session.execute(q, params).fetchall()

    if res:
        # assign to dataframe for serialization
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to viz statuses
        df.STATUS.replace(to_replace=_viz_label_mapping, inplace=True)
        df = df.to_json()
        resp = jsonify(workflow_tasks=df)
    else:
        df = pd.DataFrame(
            {}, columns=["TASK_ID", "TASK_NAME", "STATUS", "RETRIES"])
        resp = jsonify(workflow_tasks=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/task_status', methods=['GET'])
def get_task_status():

    task_ids = request.args.getlist('task_ids')
    params = {'task_ids': task_ids}
    where_clause = "task.id IN :task_ids"

    # status is an optional arg
    status_request = request.args.getlist('status', None)
    if len(status_request) > 0:
        status_codes = [i for arg in status_request
                        for i in _reversed_task_instance_label_mapping[arg]]
        params['status'] = status_codes
        where_clause += " AND task_instance.status IN :status"

    q = """
        SELECT
            task.id AS TASK_ID,
            task.status AS task_status,
            task_instance.id AS TASK_INSTANCE_ID,
            executor_id AS EXECUTOR_ID,
            task_instance_status.label AS STATUS,
            usage_str AS RESOURCE_USAGE,
            description AS ERROR_TRACE
        FROM task
        JOIN task_instance
            ON task.id = task_instance.task_id
        JOIN task_instance_status
            ON task_instance.status = task_instance_status.id
        JOIN executor_parameter_set
            ON task_instance.executor_parameter_set_id = executor_parameter_set.id
        LEFT JOIN task_instance_error_log
            ON task_instance.id = task_instance_error_log.task_instance_id
        WHERE
            {where_clause}""".format(where_clause=where_clause)
    res = DB.session.execute(q, params).fetchall()

    if res:
        # assign to dataframe for serialization
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to viz statuses
        df.STATUS.replace(to_replace=_task_instance_label_mapping, inplace=True)
        df = df[["TASK_INSTANCE_ID", "EXECUTOR_ID", "STATUS", "RESOURCE_USAGE",
                 "ERROR_TRACE"]]
        resp = jsonify(task_instance_status=df.to_json())
    else:
        df = pd.DataFrame(
            {},
            columns=["TASK_INSTANCE_ID", "EXECUTOR_ID", "STATUS",
                     "RESOURCE_USAGE", "ERROR_TRACE"])
        resp = jsonify(task_instance_status=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow/<workflow_id>/usernames', methods=['GET'])
def get_workflow_users(workflow_id: int):
    """
    Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """

    query = """
        SELECT DISTINCT user
        FROM workflow_run
        WHERE workflow_run.workflow_id = {workflow_id}
    """.format(workflow_id=workflow_id)

    result = DB.session.execute(query)

    usernames = [row.user for row in result]
    resp = jsonify(usernames=usernames)

    resp.status_code = StatusCodes.OK
    return resp


def _get_node_downstream(node_id: int, dag_id: int) -> list:
    """
    Get all downstream nodes of a node
    :param node_id:
    :return: a list of node_id
    """
    q = f"""
        SELECT downstream_node_ids
        FROM edge
        WHERE dag_id = {dag_id}
        AND node_id = {node_id}
    """
    result = DB.session.execute(q).fetchone()
    if result is None or result['downstream_node_ids'] is None:
        return None
    ids = result['downstream_node_ids'].strip()[1:-1].split(",")
    ids = [int(i) for i in ids]
    return ids


def _get_subdag(node_id: int, dag_id: int) -> list:
    """
    Get all descendants of a given nodes. It only queries the primary keys on the edge table without join.
    :param node_id:
    :return: a list of node_id
    """
    node_stack = [node_id]
    node_descendants = []
    while len(node_stack) > 0:
        node = node_stack.pop()
        node_descendants.append(node)
        node_kids = _get_node_downstream(node, dag_id)
        if node_kids is not None:
            node_stack = node_stack + node_kids
    return node_descendants


def _get_tasks_from_nodes(workflow_id: int, nodes: list, task_status: list)-> dict:
    """
    Get task ids of the given node ids
    :param workflow_id:
    :param nodes:
    :return: a dict of {<id>: <status>}
    """
    node_str =str((tuple(nodes))).replace(",)", ")")

    q = f"""
        SELECT id, status
        FROM task
        WHERE workflow_id={workflow_id}
        AND node_id in {node_str}
    """
    result = DB.session.execute(q).fetchall()
    task_dict = {}
    for r in result:
        if len(task_status) == 0:
            task_dict[int(r[0])] = r[1]
        else:
            if r[1] in task_status:
                task_dict[int(r[0])] = r[1]
    return task_dict


@jvs.route('/task/<task_id>/subdag', methods=['GET'])
def get_task_subdag(task_id: int):
    """
    Used to get the sub dag  of a given task. It returns a list of sub tasks as well as a list of sub nodes.
    :param task_id:
    :return:
    """
    # Only return sub tasks in the following status. If empty or None, return all
    task_status = request.args.getlist('task_status')
    if task_status is None:
        task_status = []
    q = f"""
        SELECT workflow.id as workflow_id, dag_id, node_id 
        FROM task, workflow 
        WHERE task.id ={task_id} and task.workflow_id = workflow.id
    """
    result = DB.session.execute(q).fetchone()
    if result is None:
        # return empty values when task_id does not exist or db out of consistency
        resp = jsonify(workflow_id=None, sub_task=None)
        resp.status_code = StatusCodes.OK
        return resp

    workflow_id = result['workflow_id']
    dag_id = result['dag_id']
    node_id = result['node_id']
    sub_dag_tree = _get_subdag(node_id, dag_id)
    sub_task_tree = _get_tasks_from_nodes(workflow_id, sub_dag_tree, task_status)
    resp = jsonify(workflow_id=workflow_id, sub_task=sub_task_tree)

    resp.status_code = StatusCodes.OK
    return resp

