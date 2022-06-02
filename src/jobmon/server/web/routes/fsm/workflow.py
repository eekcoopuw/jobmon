"""Routes for Workflows."""
from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict, Tuple

from flask import jsonify, request
import sqlalchemy
from sqlalchemy import insert, func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
import structlog

from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_attribute import WorkflowAttribute
from jobmon.server.web.models.workflow_attribute_type import WorkflowAttributeType
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.fsm import blueprint
from jobmon.server.web.server_side_exception import InvalidUsage


logger = structlog.get_logger(__name__)


def _add_workflow_attributes(
    workflow_id: int, workflow_attributes: Dict[str, str], session: Session
) -> None:
    # add attribute
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)
    logger.info(f"Add Attributes: {workflow_attributes}")
    wf_attributes_list = []
    with session.begin_nested():
        for name, val in workflow_attributes.items():
            wf_type_id = _add_or_get_wf_attribute_type(name, session)
            wf_attribute = WorkflowAttribute(
                workflow_id=workflow_id, workflow_attribute_type_id=wf_type_id, value=val
            )
            wf_attributes_list.append(wf_attribute)
            logger.debug(f"Attribute name: {name}, value: {val}")
        session.add_all(wf_attributes_list)


@blueprint.route("/workflow", methods=["POST"])
def bind_workflow() -> Any:
    """Bind a workflow to the database."""
    try:
        data = cast(Dict, request.get_json())
        tv_id = int(data["tool_version_id"])
        dag_id = int(data["dag_id"])
        whash = str(data["workflow_args_hash"])
        thash = str(data["task_hash"])
        description = data["description"]
        name = data["name"]
        workflow_args = data["workflow_args"]
        max_concurrently_running = data["max_concurrently_running"]
        workflow_attributes = data["workflow_attributes"]

    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    structlog.threadlocal.bind_threadlocal(
        dag_id=dag_id,
        tool_version_id=tv_id,
        workflow_args_hash=str(whash),
        task_hash=str(thash),
    )
    logger.info("Bind workflow")
    session = SessionLocal()
    with session.begin():

        select_stmt = select(
            Workflow
        ).where(
            Workflow.tool_version_id == tv_id,
            Workflow.dag_id == dag_id,
            Workflow.workflow_args_hash == whash,
            Workflow.task_hash == thash,
        )
        workflow = session.execute(select_stmt).scalars().one_or_none()
        if workflow is None:
            # create a new workflow
            workflow = Workflow(
                tool_version_id=tv_id,
                dag_id=dag_id,
                workflow_args_hash=whash,
                task_hash=thash,
                description=description,
                name=name,
                workflow_args=workflow_args,
                max_concurrently_running=max_concurrently_running,
            )
            session.add(workflow)
            session.flush()
            logger.info("Created new workflow")

            # update attributes
            if workflow_attributes:
                _add_workflow_attributes(workflow.id, workflow_attributes, session)
                session.flush()
            newly_created = True
        else:
            newly_created = False

    resp = jsonify(
        {
            "workflow_id": workflow.id,
            "status": workflow.status,
            "newly_created": newly_created,
        }
    )
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_args_hash>", methods=["GET"])
def get_matching_workflows_by_workflow_args(workflow_args_hash: str) -> Any:
    """Return any dag hashes that are assigned to workflows with identical workflow args."""
    try:
        workflow_args_hash = str(int(workflow_args_hash))
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    structlog.threadlocal.bind_threadlocal(workflow_args_hash=str(workflow_args_hash))
    logger.info(f"Looking for wf with hash {workflow_args_hash}")

    session = SessionLocal()
    with session.begin():

        select_stmt = select(
            Workflow.task_hash, Workflow.tool_version_id, Dag.hash
        ).join_from(
            Workflow, Dag, Workflow.dag_id == Dag.id
        ).where(
            Workflow.workflow_args_hash == workflow_args_hash
        )
        res = []
        for row in session.execute(select_stmt).all():
            res.append((row.task_hash, row.tool_version_id, row.hash))

    if len(res) > 0:
        logger.debug(f"Found {res} workflow for " f"workflow_args_hash {workflow_args_hash}")

    resp = jsonify(matching_workflows=res)
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_wf_attribute_type(name: str, session: Session) -> int:
    try:
        with session.begin_nested():
            wf_attrib_type = WorkflowAttributeType(name=name)
            session.add(wf_attrib_type)
    except sqlalchemy.exc.IntegrityError:
        with session.begin_nested():
            select_stmt = select(
                WorkflowAttributeType
            ).where(
                WorkflowAttributeType.name == name
            )
            wf_attrib_type = session.execute(select_stmt).scalars().one()

    return wf_attrib_type.id


def _upsert_wf_attribute(workflow_id: int, name: str, value: str, session: Session) -> None:
    with session.begin_nested():
        wf_attrib_id = _add_or_get_wf_attribute_type(name, session)
        if SessionLocal.bind.dialect.name == "mysql":
            insert_vals = insert(WorkflowAttribute).values(
                workflow_id=workflow_id, workflow_attribute_type_id=wf_attrib_id, value=value
            )
            upsert_stmt = insert_vals.on_duplicate_key_update(value=insert_vals.inserted.value)
        elif SessionLocal.bind.dialect.name == "sqlite":
            insert_vals = sqlite_insert(WorkflowAttribute).values(
                workflow_id=workflow_id, workflow_attribute_type_id=wf_attrib_id, value=value
            )
            upsert_stmt = insert_vals.on_conflict_do_update(
                index_elements=['workflow_id', 'workflow_attribute_type_id'],
                set_=dict(value=value)
            )
        session.execute(upsert_stmt)


@blueprint.route("/workflow/<workflow_id>/workflow_attributes", methods=["PUT"])
def update_workflow_attribute(workflow_id: int) -> Any:
    """Update the attributes for a given workflow."""
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)
    try:
        workflow_id = int(workflow_id)
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e
    """ Add/update attributes for a workflow """
    data = cast(Dict, request.get_json())

    logger.debug("Update attributes")
    attributes = data["workflow_attributes"]
    if attributes:
        session = SessionLocal()
        with session.begin():
            for name, val in attributes.items():
                _upsert_wf_attribute(workflow_id, name, val, session)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/set_resume", methods=["POST"])
def set_resume(workflow_id: int) -> Any:
    """Set resume on a workflow."""
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)
    try:
        data = cast(Dict, request.get_json())
        logger.info("Set resume for workflow")
        reset_running_jobs = bool(data["reset_running_jobs"])
        description = str(data["description"])
        name = str(data["name"])
        max_concurrently_running = int(data["max_concurrently_running"])
        workflow_attributes = data["workflow_attributes"]
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    session = SessionLocal()
    with session.begin():
        select_stmt = select(
            Workflow
        ).where(
            Workflow.id == workflow_id
        )
        workflow = session.execute(select_stmt).scalars().one()

        # set mutable attribute
        workflow.description = description
        workflow.name = name
        workflow.max_concurrently_running = max_concurrently_running
        session.flush()

        # trigger resume on active workflow run
        workflow.resume(reset_running_jobs)
        session.flush()
        logger.info(f"Resume set for wf {workflow_id}")

        # upsert attributes
        if workflow_attributes:
            logger.info("Upsert attributes for workflow")
            if workflow_attributes:
                for name, val in workflow_attributes.items():
                    _upsert_wf_attribute(workflow_id, name, val, session)

    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/is_resumable", methods=["GET"])
def workflow_is_resumable(workflow_id: int) -> Any:
    """Check if a workflow is in a resumable state."""
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)

    session = SessionLocal()
    with session.begin():
        select_stmt = select(
            Workflow
        ).where(
            Workflow.id == workflow_id
        )
        workflow = session.execute(select_stmt).scalars().one()

    logger.info(f"Workflow is resumable: {workflow.is_resumable}")
    resp = jsonify(workflow_is_resumable=workflow.is_resumable)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow/<workflow_id>/get_max_concurrently_running", methods=["GET"])
def get_max_concurrently_running(workflow_id: int) -> Any:
    """Return the maximum concurrency of this workflow."""
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)

    session = SessionLocal()
    with session.begin():
        select_stmt = select(
            Workflow
        ).where(
            Workflow.id == workflow_id
        )
        workflow = session.execute(select_stmt).scalars().one()

    resp = jsonify(max_concurrently_running=workflow.max_concurrently_running)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "workflow/<workflow_id>/update_max_concurrently_running", methods=["PUT"]
)
def update_max_running(workflow_id: int) -> Any:
    """Update the number of tasks that can be running concurrently for a given workflow."""
    data = cast(Dict, request.get_json())
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)
    logger.debug("Update workflow max concurrently running")

    try:
        new_limit = data["max_tasks"]
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    session = SessionLocal()
    with session.begin():
        update_stmt = update(
            Workflow
        ).where(
            Workflow.id == workflow_id
        ).values(
            max_concurrently_running=new_limit
        )
        res = session.execute(update_stmt)

    if res.rowcount == 0:  # Return a warning message if no update was performed
        message = (
            f"No update performed for workflow ID {workflow_id}, max_concurrently_running is "
            f"{new_limit}"
        )
    else:
        message = f"Workflow ID {workflow_id} max concurrently running updated to {new_limit}"

    resp = jsonify(message=message)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "/workflow/<workflow_id>/task_status_updates", methods=["POST"]
)
def task_status_updates(workflow_id: int) -> Any:
    """Returns all tasks in the database that have the specified status.

    Args:
        workflow_id (int): the ID of the workflow.
    """
    structlog.threadlocal.bind_threadlocal(workflow_id=workflow_id)
    data = cast(Dict, request.get_json())
    logger.info("Get task by status")

    try:
        filter_criteria: Tuple = (
            (Task.workflow_id == workflow_id),
            (Task.status_date >= data["last_sync"])
        )
    except KeyError:
        filter_criteria = (Task.workflow_id == workflow_id,)

    # get time from db
    session = SessionLocal()
    with session.begin():

        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")

        tasks_by_status_query = (
            select(Task.status, func.group_concat(Task.id))
            .where(*filter_criteria)
            .group_by(Task.status)
        )
        result_dict = {}
        for row in session.execute(tasks_by_status_query):
            result_dict[row[0]] = [int(i) for i in row[1].split(",")]

    resp = jsonify(tasks_by_status=result_dict, time=str_time)
    resp.status_code = StatusCodes.OK
    return resp
