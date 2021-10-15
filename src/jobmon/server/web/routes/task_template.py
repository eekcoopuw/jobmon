"""Routes for Tasks."""
from functools import partial
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify, request
import sqlalchemy
from sqlalchemy.sql import text
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.serializers import SerializeTaskTemplateResourceUsage
from jobmon.server.web.models import DB
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.arg_type import ArgType
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.template_arg_map import TemplateArgMap
from jobmon.server.web.routes import finite_state_machine
from jobmon.server.web.server_side_exception import InvalidUsage

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))

import numpy as np

import scipy.stats as st

import sqlalchemy
from sqlalchemy.sql import text

from . import jobmon_cli, jobmon_client


@finite_state_machine.route("/task_template", methods=["POST"])
def get_task_template() -> Any:
    """Add a task template for a given tool to the database."""
    # check input variable
    data = request.get_json()
    try:
        tool_version_id = int(data["tool_version_id"])
        bind_to_logger(tool_version_id=tool_version_id)
        logger.info(f"Add task tamplate for tool_version_id {tool_version_id} ")

    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    # add to DB
    try:
        tt = TaskTemplate(
            tool_version_id=data["tool_version_id"], name=data["task_template_name"]
        )
        DB.session.add(tt)
        DB.session.commit()
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM task_template
            WHERE
                tool_version_id = :tool_version_id
                AND name = :name
        """
        tt = (
            DB.session.query(TaskTemplate)
            .from_statement(text(query))
            .params(
                tool_version_id=data["tool_version_id"], name=data["task_template_name"]
            )
            .one()
        )
        DB.session.commit()
    resp = jsonify(task_template_id=tt.id)
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route(
    "/task_template/<task_template_id>/versions", methods=["GET"]
)
def get_task_template_versions(task_template_id: int) -> Any:
    """Get the task_template_version."""
    # get task template version object
    bind_to_logger(task_template_id=task_template_id)
    logger.info(f"Getting task template version for task template: {task_template_id}")

    # parse args
    command_template = request.args.get("command_template")
    arg_mapping_hash = request.args.get("arg_mapping_hash")
    query = """
        SELECT
            task_template_version.*
        FROM task_template_version
        WHERE
            task_template_id = :task_template_id
    """
    ttvs = (
        DB.session.query(TaskTemplateVersion)
        .from_statement(text(query))
        .params(task_template_id=task_template_id)
        .all()
    )

    wire_obj = [ttv.to_wire_as_client_task_template_version() for ttv in ttvs]

    resp = jsonify(task_template_versions=wire_obj)
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/get_task_template_version', methods=['GET'])
def get_task_template_version_for_tasks():
    """Get the task_template_version_ids."""
    # parse args
    t_id = request.args.get("task_id")
    wf_id = request.args.get("workflow_id")
    app.logger.debug(f"Task ID: {t_id}; Workflow ID: {wf_id}")

    # This route only accept one task id or one wf id;
    # If provided both, ignor wf id
    if t_id:
        sql = f"""
               SELECT task_template_version_id as id, t4.name as name
               FROM task t1, node t2, task_template_version t3, task_template t4
               WHERE t1.id = {t_id}
               AND t1.node_id=t2.id
               AND t2.task_template_version_id=t3.id
               AND t3.task_template_id=t4.id
        """
    else:
        sql = f"""
               SELECT distinct task_template_version_id as id, t4.name as name
               FROM task t1, node t2, task_template_version t3, task_template t4
               WHERE t1.workflow_id = {wf_id}
               AND t1.node_id=t2.id
               AND t2.task_template_version_id=t3.id
               AND t3.task_template_id=t4.id
        """

    rows = DB.session.execute(sql).fetchall()
    # return a "standard" json format for cli routes so that it can be reused by future GUI
    ttvis = [] if rows is None else [{"id": r["id"], "name": r["name"]} for r in rows]
    resp = jsonify({"task_template_version_ids": ttvis})
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/get_requested_cores', methods=['GET'])
def get_requsted_cores():
    """Get the min, max, and arg of requested cores"""
    # parse args
    ttvis = request.args.get("task_template_version_ids")
    app.logger.debug(f"task_template_version_ids: {ttvis}")
    # null core should be treated as 1 instead of 0
    sql = f"""
           SELECT t3.id as id,
                  min(IFNULL(num_cores, 1)) as min,
                  max(IFNULL(num_cores, 1)) as max,
                  avg(IFNULL(num_cores, 1)) as avg
           FROM task t1, node t2, task_template_version t3,  executor_parameter_set t4
           WHERE t3.id in {ttvis}
           AND t2.task_template_version_id=t3.id
           AND t1.node_id=t2.id
           AND t4.task_id=t1.id
           GROUP BY t3.id;
    """
    rows = DB.session.execute(sql).fetchall()
    # return a "standard" json format for cli routes so that it can be reused by future GUI
    core_info = [] if rows is None else \
        [{"id": r["id"],
          "min": int(r["min"]),
          "max": int(r["max"]),
          "avg": int(round(r["avg"]))} for r in rows]
    resp = jsonify({"core_info": core_info})
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_cli.route('/get_most_popular_queue', methods=['GET'])
def get_most_popular_queue():
    """Get the min, max, and arg of requested cores"""
    # parse args
    ttvis = request.args.get("task_template_version_ids")
    app.logger.debug(f"task_template_version_ids: {ttvis}")
    sql = f"""
           SELECT task_template_version_id, queue
           FROM
                (SELECT t3.id as task_template_version_id, queue, count(*) as c
                FROM task t1, node t2, task_template_version t3, executor_parameter_set t4
                WHERE t3.id in {ttvis}
                AND t2.task_template_version_id=t3.id
                AND t1.node_id=t2.id
                AND t4.task_id=t1.id GROUP BY t3.id, queue ORDER BY c desc) as t
           GROUP BY t.task_template_version_id
    """
    rows = DB.session.execute(sql).fetchall()
    # return a "standard" json format for cli routes
    queue_info = [] if rows is None else \
        [{"id": r["task_template_version_id"],
          "queue": r["queue"]} for r in rows]
    resp = jsonify({"queue_info": queue_info})
    resp.status_code = StatusCodes.OK
    return resp


def _add_or_get_arg(name: str) -> Arg:
    try:
        arg = Arg(name=name)
        DB.session.add(arg)
        DB.session.commit()
        app.logger.debug(f"Arg {name} added for ID {arg.id}")
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM arg
            WHERE name = :name
        """
        arg = DB.session.query(Arg).from_statement(text(query)).params(name=name).one()
        DB.session.commit()
        app.logger.debug(f"Found {name} arg in the database with ID {arg.id}")
    return arg


@finite_state_machine.route(
    "/task_template/<task_template_id>/add_version", methods=["POST"]
)
def add_task_template_version(task_template_id: int) -> Any:
    """Add a tool to the database."""
    # check input variables
    bind_to_logger(task_template_id=task_template_id)
    data = request.get_json()
    logger.info(f"Add tool for task_template_id {task_template_id}")
    if task_template_id is None:
        raise InvalidUsage(
            f"Missing variable task_template_id in {request.path}", status_code=400
        )
    try:
        int(task_template_id)
        # populate the argument table
        arg_mapping_dct: dict = {
            ArgType.NODE_ARG: [],
            ArgType.TASK_ARG: [],
            ArgType.OP_ARG: [],
        }
        for arg_name in data["node_args"]:
            arg_mapping_dct[ArgType.NODE_ARG].append(_add_or_get_arg(arg_name))
        for arg_name in data["task_args"]:
            arg_mapping_dct[ArgType.TASK_ARG].append(_add_or_get_arg(arg_name))
        for arg_name in data["op_args"]:
            arg_mapping_dct[ArgType.OP_ARG].append(_add_or_get_arg(arg_name))
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to {request.path}", status_code=400
        ) from e

    try:
        ttv = TaskTemplateVersion(
            task_template_id=task_template_id,
            command_template=data["command_template"],
            arg_mapping_hash=data["arg_mapping_hash"],
        )
        DB.session.add(ttv)
        DB.session.commit()
        app.logger.debug(f"TTV {ttv.id} added for task template {task_template_id}")

        # get a lock
        DB.session.refresh(ttv, with_for_update=True)

        for arg_type_id in arg_mapping_dct.keys():
            for arg in arg_mapping_dct[arg_type_id]:
                ctatm = TemplateArgMap(
                    task_template_version_id=ttv.id,
                    arg_id=arg.id,
                    arg_type_id=arg_type_id,
                )
                DB.session.add(ctatm)
        DB.session.commit()
        resp = jsonify(
            task_template_version=ttv.to_wire_as_client_task_template_version()
        )
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        # if another process is adding this task_template_version then this query should block
        # until the template_arg_map has been populated and committed
        query = """
            SELECT *
            FROM task_template_version
            WHERE
                task_template_id = :task_template_id
                AND command_template = :command_template
                AND arg_mapping_hash = :arg_mapping_hash
        """
        ttv = (
            DB.session.query(TaskTemplateVersion)
            .from_statement(text(query))
            .params(
                task_template_id=task_template_id,
                command_template=data["command_template"],
                arg_mapping_hash=data["arg_mapping_hash"],
            )
            .one()
        )
        DB.session.commit()
        resp = jsonify(
            task_template_version=ttv.to_wire_as_client_task_template_version()
        )
        resp.status_code = StatusCodes.OK
        return resp


@jobmon_client.route('/task_template_resource_usage', methods=["POST"])
def get_task_template_resource_usage():
    """Return the aggregate resource usage for a give TaskTemplate."""
    data = request.get_json()
    try:
        task_template_version_id = data.pop("task_template_version_id")
    except Exception as e:
        raise InvalidUsage(f"{str(e)} in request to /task_template_resource_usage",
                           status_code=400) from e

    params = {'task_template_version_id': task_template_version_id}
    where_clause = ""
    from_clause = ""
    workflows = data.pop("workflows", None)
    node_args = data.pop("node_args", None)
    ci = data.pop("ci", None)
    if workflows:
        from_clause += ", workflow_run AS wfr, workflow AS wf"
        where_clause += " AND ti.workflow_run_id = wfr.id AND wfr.workflow_id = wf.id AND " \
                        "wf.id IN :workflows"
        params['workflows'] = workflows

    if node_args:
        from_clause += ", arg AS a, node_arg AS na"
        where_clause += " AND t.node_id = na.node_id AND a.id = na.arg_id AND ("

        def construct_arg_list_clause(k, v):
            quoted_arg_vals = ",".join(f"'{x}'" for x in v)
            return f"(a.name = '{k}' AND na.val IN ({quoted_arg_vals}))"

        where_clause += " OR ".join(construct_arg_list_clause(key, value) for key, value in
                                    node_args.items())
        where_clause += ")"

    query = """
              SELECT
                    ti.wallclock as r,
                    ti.maxpss as m
                FROM
                    task_template_version AS ttv,
                    node AS n,
                    task AS t,
                    task_instance AS ti {from_clause}
                WHERE
                    ttv.id = :task_template_version_id
                    AND t.status = 'D'
                    AND ti.status = 'D'
                    AND ttv.id = n.task_template_version_id
                    AND n.id = t.node_id
                    AND t.id = ti.task_id {where_clause}
    """.format(from_clause=from_clause, where_clause=where_clause)
    result = DB.session.execute(query, params).fetchall()
    if result is None or len(result) == 0:
        resource_usage = SerializeTaskTemplateResourceUsage.to_wire(None, None, None, None,
                                                                    None, None, None, None,
                                                                    None, None, None)
    else:
        runtimes = []
        mems = []
        for row in result:
            runtimes.append(int(row["r"]))
            mems.append(int(row["m"]))
        num_tasks = len(runtimes)
        min_mem = int(np.min(mems))
        max_mem = int(np.max(mems))
        mean_mem = round(float(np.mean(mems)), 2)
        min_runtime = int(np.min(runtimes))
        max_runtime = int(np.max(runtimes))
        mean_runtime = round(float(np.mean(runtimes)), 2)
        median_mem = round(float(np.percentile(runtimes, 50)), 2)
        median_runtime = round(float(np.percentile(mems, 50)), 2)

        if ci is None:
            ci_mem = [float("nan"), float("nan")]
            ci_runtime = [float("nan"), float("nan")]
        else:
            try:
                ci = float(ci)

                def _calculate_ci(d, ci):
                    interval = st.t.interval(alpha=ci, df=len(d) - 1,
                                             loc=np.mean(d), scale=st.sem(d))
                    # we only need the higher end
                    return round(float(interval[0]), 2), round(float(interval[1]), 2)

                ci_mem = _calculate_ci(mems, ci)
                ci_runtime = _calculate_ci(runtimes, ci)

            except ValueError:
                app.logger.warn(f"Unable to convert {ci} to float. Use NaN.")
                ci_mem = [float("nan"), float("nan")]
                ci_runtime = [float("nan"), float("nan")]

        resource_usage = SerializeTaskTemplateResourceUsage.to_wire(num_tasks,
                                                                    min_mem,
                                                                    max_mem,
                                                                    mean_mem,
                                                                    min_runtime,
                                                                    max_runtime,
                                                                    mean_runtime,
                                                                    median_mem,
                                                                    median_runtime,
                                                                    ci_mem,
                                                                    ci_runtime)
    resp = jsonify(resource_usage)
    resp.status_code = StatusCodes.OK
    return resp
