"""Routes for Workflows"""
from http import HTTPStatus as StatusCodes
from typing import Dict


from flask import current_app as app, jsonify, request

from jobmon.constants import WorkflowStatus as Statuses
from jobmon.server.web.models import DB
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.server_side_exception import InvalidUsage

import pandas as pd

import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import text

from . import jobmon_cli, jobmon_client, jobmon_swarm

_cli_label_mapping = {
    "A": "PENDING",
    "G": "PENDING",
    "Q": "PENDING",
    "I": "PENDING",
    "E": "PENDING",
    "R": "RUNNING",
    "F": "FATAL",
    "D": "DONE"
}

_reversed_cli_label_mapping = {
    "PENDING": ["A", "G", "Q", "I", "E"],
    "RUNNING": ["R"],
    "FATAL": ["F"],
    "DONE": ["D"]
}

_cli_order = ["PENDING", "RUNNING", "DONE", "FATAL"]


def _add_workflow_attributes(workflow_id: int, workflow_attributes: Dict[str, str]):
    # add attribute
    wf_attributes_list = []
    for name, val in workflow_attributes.items():
        wf_type_id = _add_or_get_wf_attribute_type(name)
        wf_attribute = WorkflowAttribute(workflow_id=workflow_id,
                                         workflow_attribute_type_id=wf_type_id,
                                         value=val)
        wf_attributes_list.append(wf_attribute)
    DB.session.add_all(wf_attributes_list)
    DB.session.flush()


@jobmon_client.route('/workflow', methods=['POST'])
def bind_workflow():
    """Bind a workflow to the database."""
    data = request.get_json()
    try:
        data = request.get_json()
        tv_id = int(data['tool_version_id'])
        dag_id = int(data['dag_id'])
        whash = int(data['workflow_args_hash'])
        thash = int(data['task_hash'])
        description = data['description']
        name = data["name"]
        workflow_args = data["workflow_args"]
        max_concurrently_running = data['max_concurrently_running']
        workflow_attributes = data["workflow_attributes"]
        app.logger = app.logger.bind(dag_id=dag_id, tool_version_id=tv_id,
                                     workflow_args_hash=whash, task_hash=thash)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    query = """
        SELECT workflow.*
        FROM workflow
        WHERE
            tool_version_id = :tool_version_id
            AND dag_id = :dag_id
            AND workflow_args_hash = :workflow_args_hash
            AND task_hash = :task_hash
    """
    workflow = DB.session.query(Workflow).from_statement(text(query)).params(
        tool_version_id=tv_id,
        dag_id=dag_id,
        workflow_args_hash=whash,
        task_hash=thash
    ).one_or_none()
    if workflow is None:
        # create a new workflow
        workflow = Workflow(tool_version_id=tv_id,
                            dag_id=dag_id,
                            workflow_args_hash=whash,
                            task_hash=thash,
                            description=description,
                            name=name,
                            workflow_args=workflow_args,
                            max_concurrently_running=max_concurrently_running)
        DB.session.add(workflow)
        DB.session.commit()

        # update attributes
        if workflow_attributes:
            _add_workflow_attributes(workflow.id, workflow_attributes)
            DB.session.commit()

        newly_created = True
    else:
        newly_created = False

    resp = jsonify({'workflow_id': workflow.id, 'status': workflow.status,
                    'newly_created': newly_created})
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow/<workflow_args_hash>', methods=['GET'])
def get_matching_workflows_by_workflow_args(workflow_args_hash: int):
    """Return any dag hashes that are assigned to workflows with identical workflow args."""
    try:
        int(workflow_args_hash)
        app.logger = app.logger.bind(workflow_args_hash=workflow_args_hash)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    query = """
        SELECT workflow.task_hash, workflow.tool_version_id, dag.hash
        FROM workflow
        JOIN dag
            ON workflow.dag_id = dag.id
        WHERE
            workflow.workflow_args_hash = :workflow_args_hash
    """

    res = DB.session.query(Workflow.task_hash, Workflow.tool_version_id,
                           Dag.hash).from_statement(text(query)).params(
        workflow_args_hash=workflow_args_hash
    ).all()
    DB.session.commit()
    res = [(row.task_hash, row.tool_version_id, row.hash) for row in res]
    resp = jsonify(matching_workflows=res)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_wf_attribute_type(name: str) -> int:
    try:
        wf_attrib_type = WorkflowAttributeType(name=name)
        DB.session.add(wf_attrib_type)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
        SELECT id, name
        FROM workflow_attribute_type
        WHERE name = :name
        """
        wf_attrib_type = DB.session.query(WorkflowAttributeType)\
            .from_statement(text(query)).params(name=name).one()

    return wf_attrib_type.id


def _upsert_wf_attribute(workflow_id: int, name: str, value: str):
    wf_attrib_id = _add_or_get_wf_attribute_type(name)
    insert_vals = insert(WorkflowAttribute).values(
        workflow_id=workflow_id,
        workflow_attribute_type_id=wf_attrib_id,
        value=value
    )

    upsert_stmt = insert_vals.on_duplicate_key_update(
        value=insert_vals.inserted.value,
        status='U')

    DB.session.execute(upsert_stmt)
    DB.session.commit()


@jobmon_client.route('/workflow/<workflow_id>/workflow_attributes', methods=['PUT'])
def update_workflow_attribute(workflow_id: int):
    """Update the attributes for a given workflow."""
    app.logger = app.logger.bind(workflow_id=workflow_id)
    try:
        int(workflow_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    """ Add/update attributes for a workflow """
    data = request.get_json()
    attributes = data["workflow_attributes"]
    if attributes:
        for name, val in attributes.items():
            _upsert_wf_attribute(workflow_id, name, val)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow/<workflow_id>/set_resume', methods=['POST'])
def set_resume(workflow_id: int):
    """Set resume on a workflow"""
    app.logger = app.logger.bind(workflow_id=workflow_id)
    try:
        data = request.get_json()
        reset_running_jobs = bool(data['reset_running_jobs'])
        description = str(data['description'])
        name = str(data["name"])
        max_concurrently_running = int(data['max_concurrently_running'])
        workflow_attributes = data["workflow_attributes"]
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    query = """
        SELECT
            workflow.*
        FROM
            workflow
        WHERE
            workflow.id = :workflow_id
    """
    workflow = DB.session.query(Workflow).from_statement(text(query)).params(
        workflow_id=workflow_id
    ).one()

    # set mutible attribute
    workflow.description = description
    workflow.name = name
    workflow.max_concurrently_running = max_concurrently_running
    DB.session.commit()

    # trigger resume on active workflow run
    workflow.resume(reset_running_jobs)
    DB.session.commit()

    # update attributes
    if workflow_attributes:
        _add_workflow_attributes(workflow.id, workflow_attributes)
        DB.session.commit()

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/workflow/<workflow_id>/is_resumable', methods=['GET'])
def workflow_is_resumable(workflow_id: int):
    """Check if a workflow is in a resumable state"""
    app.logger = app.logger.bind(workflow_id=workflow_id)
    query = """
        SELECT
            workflow.*
        FROM
            workflow
        WHERE
            workflow.id = :workflow_id
    """
    workflow = DB.session.query(Workflow).from_statement(text(query)).params(
        workflow_id=workflow_id
    ).one()
    DB.session.commit()

    resp = jsonify(workflow_is_resumable=workflow.is_resumable)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('workflow/<workflow_id>/update_max_running', methods=['PUT'])
def update_max_running(workflow_id):
    """Update the number of tasks that can be running concurrently for a given workflow."""
    data = request.get_json()
    try:
        new_limit = data['max_tasks']
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    q = """
        UPDATE workflow
        SET max_concurrently_running = {new_limit}
        WHERE id = {workflow_id}
    """.format(new_limit=new_limit, workflow_id=workflow_id)

    res = DB.session.execute(q)
    DB.session.commit()

    if res.rowcount == 0:  # Return a warning message if no update was performed
        message = f"No update performed for workflow ID {workflow_id}, max_concurrency is " \
                  f"{new_limit}"
    else:
        message = f"Workflow ID {workflow_id} max concurrency updated to {new_limit}"

    resp = jsonify(message=message)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_swarm.route('/workflow/<workflow_id>/task_status_updates', methods=['POST'])
def get_task_by_status_only(workflow_id: int):
    """Returns all tasks in the database that have the specified status

    Args:
        status (str): status to query for
        last_sync (datetime): time since when to get tasks
    """
    app.logger = app.logger.bind(workflow_id=workflow_id)
    data = request.get_json()

    last_sync = data['last_sync']
    swarm_tasks_tuples = data.get('swarm_tasks_tuples', [])

    # get time from db
    db_time = DB.session.execute("SELECT CURRENT_TIMESTAMP AS t").fetchone()['t']
    str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")

    if swarm_tasks_tuples:
        # Sample swarm_tasks_tuples: [(1, 'I')]
        swarm_task_ids = ",".join([str(task_id[0]) for task_id in swarm_tasks_tuples])
        swarm_tasks_tuples = [(int(task_id), str(status))
                              for task_id, status in swarm_tasks_tuples]

        query_swarm_tasks_tuples = ""
        for task_id, status in swarm_tasks_tuples:
            query_swarm_tasks_tuples += f"({task_id},'{status}'),"
        # get rid of trailing comma on final line
        query_swarm_tasks_tuples = query_swarm_tasks_tuples[:-1]

        query = """
            SELECT
                task.id, task.status
            FROM task
            WHERE
                workflow_id = {workflow_id}
                AND (
                    (
                        task.id IN ({swarm_task_ids})
                        AND (task.id, status) NOT IN ({tuples})
                    )
                    OR status_date >= '{status_date}')
        """.format(workflow_id=workflow_id,
                   swarm_task_ids=swarm_task_ids,
                   tuples=query_swarm_tasks_tuples,
                   status_date=last_sync)
        app.logger.debug(query)
        rows = DB.session.query(Task).from_statement(text(query)).all()

    else:
        query = """
            SELECT
                task.id, task.status
            FROM task
            WHERE
                workflow_id = :workflow_id
                AND status_date >= :last_sync"""
        rows = DB.session.query(Task).from_statement(text(query)).params(
            workflow_id=workflow_id,
            last_sync=str(last_sync)
        ).all()

    DB.session.commit()
    task_dcts = [row.to_wire_as_swarm_task() for row in rows]
    app.logger.debug("task_dcts={}".format(task_dcts))
    resp = jsonify(task_dcts=task_dcts, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route("/workflow_validation", methods=['GET'])
def get_workflow_validation_status():
    """Check if workflow is valid."""
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
    if len(res) == 1 and res[0][1] in (Statuses.FAILED, Statuses.DONE, Statuses.ABORTED,
                                       Statuses.HALTED):
        validation = True
    else:
        validation = False

    resp = jsonify(validation=validation, workflow_status=res[0][1])
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/workflow_status', methods=['GET'])
def get_workflow_status():
    """Get the status of the workflow."""
    # initial params
    params = {}
    user_request = request.args.getlist('user')
    if user_request == "all":  # specifying all is equivalent to None
        user_request = []
    workflow_request = request.args.getlist('workflow_id')
    if workflow_request == "all":  # specifying all is equivalent to None
        workflow_request = []
    limit_request = request.args.getlist('limit')
    limit = None if len(limit_request) == 0 else limit_request[0]

    where_clause = ""
    # convert workflow request into sql filter
    if workflow_request:
        workflow_request = [int(w) for w in workflow_request]
        params["workflow_id"] = workflow_request
        where_clause = "WHERE workflow.id in :workflow_id "
    else:  # if we don't specify workflow then we use the users
        # convert user request into sql filter
        # directly producing workflow_ids, and thus where_clause
        if user_request:
            params["user"] = user_request
            where_clause_user = "WHERE workflow_run.user in :user "
            q_user = """
                SELECT DISTINCT workflow_id
                FROM workflow_run
                {where_clause}
            """.format(where_clause=where_clause_user)
            res_user = DB.session.execute(q_user, params).fetchall()
            workflow_ids = [int(row.workflow_id) for row in res_user]
            params["workflow_id"] = workflow_ids
            where_clause = "WHERE workflow.id in :workflow_id "

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
        JOIN task
            ON workflow.id = task.workflow_id
        JOIN workflow_status
            ON workflow_status.id = workflow.status
        {where_clause}
        GROUP BY workflow.id, task.status, workflow.name, workflow_status.label
        ORDER BY workflow.id desc
    """.format(where_clause=where_clause)
    if limit:
        q = f"{q}\nLIMIT {limit}"
    res = DB.session.execute(q, params).fetchall()

    if res:

        # assign to dataframe for aggregation
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to jobmon_cli statuses
        df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)

        # aggregate totals by workflow and status
        df = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS", "STATUS"]
                        ).agg({'TASKS': 'sum', 'RETRIES': 'sum'})

        # pivot wide by task status
        tasks = df.pivot_table(
            values="TASKS",
            index=["WF_ID", "WF_NAME", "WF_STATUS"],
            columns="STATUS",
            fill_value=0)
        for col in _cli_order:
            if col not in tasks.columns:
                tasks[col] = 0
        tasks = tasks[_cli_order]

        # aggregate again without status to get the totals by workflow
        retries = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS"]
                             ).agg({'TASKS': 'sum', 'RETRIES': 'sum'})

        # combine datasets
        df = pd.concat([tasks, retries], axis=1)

        # compute pcts and format
        for col in _cli_order:
            df[col + "_pct"] = (
                df[col].astype(float) / df["TASKS"].astype(float)) * 100
            df[col + "_pct"] = df[[col + "_pct"]].round(1)
            df[col] = (df[col].astype(int).astype(str) + " (" + df[col + "_pct"].astype(
                str) + "%)")

        # df.replace(to_replace={"0 (0.0%)": "NA"}, inplace=True)
        # final order
        df = df[["TASKS"] + _cli_order + ["RETRIES"]]
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


@jobmon_cli.route('/workflow/<workflow_id>/workflow_tasks', methods=['GET'])
def get_workflow_tasks(workflow_id):
    """Get the tasks for a given workflow."""
    params = {"workflow_id": workflow_id}
    where_clause = "WHERE workflow.id = :workflow_id"
    status_request = request.args.getlist('status', None)

    if status_request:
        params["status"] = [i for arg in status_request
                            for i in _reversed_cli_label_mapping[arg]]
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

        # remap to jobmon_cli statuses
        df.STATUS.replace(to_replace=_cli_label_mapping, inplace=True)
        df = df.to_json()
        resp = jsonify(workflow_tasks=df)
    else:
        df = pd.DataFrame(
            {}, columns=["TASK_ID", "TASK_NAME", "STATUS", "RETRIES"])
        resp = jsonify(workflow_tasks=df.to_json())

    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/workflow/<workflow_id>/usernames', methods=['GET'])
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


@jobmon_cli.route('/workflow/<workflow_id>/validate_username', methods=['GET'])
def get_workflow_user_validation(workflow_id: int):
    """
    Return all usernames associated with a given workflow_id's workflow runs.

    Used to validate permissions for a self-service request.
    """
    user = request.args.get('username')
    query = """
        SELECT DISTINCT user
        FROM workflow_run
        WHERE workflow_run.workflow_id = {workflow_id}
    """.format(workflow_id=workflow_id)

    result = DB.session.execute(query)

    usernames = [row.user for row in result]

    resp = jsonify(validation=user in usernames)

    resp.status_code = StatusCodes.OK
    return resp
