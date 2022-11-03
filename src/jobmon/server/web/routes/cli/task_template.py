"""Routes for TaskTemplate."""
from http import HTTPStatus as StatusCodes
import json
from typing import Any, Dict, List

from flask import jsonify, request
from flask_cors import cross_origin
import numpy as np
import scipy.stats as st  # type:ignore
from sqlalchemy import select
import structlog

from jobmon.serializers import SerializeTaskTemplateResourceUsage
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.cli import blueprint
from jobmon.server.web.routes.cli.workflow import _cli_label_mapping
from jobmon.server.web.server_side_exception import InvalidUsage

# new structlog logger per flask request context. internally stored as flask.g.logger
logger = structlog.get_logger(__name__)


@blueprint.route("/get_task_template_version", methods=["GET"])
def get_task_template_version_for_tasks() -> Any:
    """Get the task_template_version_ids."""
    # parse args
    t_id = request.args.get("task_id")
    wf_id = request.args.get("workflow_id")
    # This route only accept one task id or one wf id;
    # If provided both, ignor wf id
    session = SessionLocal()
    with session.begin():
        if t_id:
            query_filter = [
                Task.id == t_id,
                Task.node_id == Node.id,
                Node.task_template_version_id == TaskTemplateVersion.id,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            ]
            sql = select(
                TaskTemplateVersion.id,
                TaskTemplate.name,
            ).where(*query_filter)

        else:
            query_filter = [
                Task.workflow_id == wf_id,
                Task.node_id == Node.id,
                Node.task_template_version_id == TaskTemplateVersion.id,
                TaskTemplateVersion.task_template_id == TaskTemplate.id,
            ]
            sql = (
                select(
                    TaskTemplateVersion.id,
                    TaskTemplate.name,
                ).where(*query_filter)
            ).distinct()
        rows = session.execute(sql).all()
    column_names = ("id", "name")
    ttvis = [dict(zip(column_names, ti)) for ti in rows]

    resp = jsonify({"task_template_version_ids": ttvis})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/get_requested_cores", methods=["GET"])
def get_requested_cores() -> Any:
    """Get the min, max, and arg of requested cores."""
    # parse args
    ttvis = request.args.get("task_template_version_ids")
    if ttvis is None:
        raise ValueError(
            "No task_template_version_ids returned in /get_requested_cores"
        )
    ttvis = [int(i) for i in ttvis[1:-1].split(",")]
    # null core should be treated as 1 instead of 0
    session = SessionLocal()
    with session.begin():
        query_filter = [
            TaskTemplateVersion.id.in_(ttvis),
            TaskTemplateVersion.id == Node.task_template_version_id,
            Task.node_id == Node.id,
            Task.task_resources_id == TaskResources.id,
        ]

        sql = select(TaskTemplateVersion.id, TaskResources.requested_resources).where(
            *query_filter
        )
    rows = session.execute(sql).all()
    column_names = ("id", "rr")
    rows = [dict(zip(column_names, ti)) for ti in rows]

    core_info = []
    if rows:
        result_dir: Dict = dict()
        for r in rows:
            # json loads hates single quotes
            j_str = r["rr"].replace("'", '"')
            j_dir = json.loads(j_str)
            core = 1 if "num_cores" not in j_dir.keys() else int(j_dir["num_cores"])
            if r["id"] in result_dir.keys():
                result_dir[r["id"]].append(core)
            else:
                result_dir[r["id"]] = [core]
        for k in result_dir.keys():
            item_min = int(np.min(result_dir[k]))
            item_max = int(np.max(result_dir[k]))
            item_mean = round(np.mean(result_dir[k]))
            core_info.append(
                {"id": k, "min": item_min, "max": item_max, "avg": item_mean}
            )

    resp = jsonify({"core_info": core_info})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/get_most_popular_queue", methods=["GET"])
def get_most_popular_queue() -> Any:
    """Get the most popular queue of the task template."""
    # parse args
    ttvis = request.args.get("task_template_version_ids")
    if ttvis is None:
        raise ValueError(
            "No task_template_version_ids returned in /get_most_popular_queue."
        )
    ttvis = [int(i) for i in ttvis[1:-1].split(",")]
    session = SessionLocal()
    with session.begin():
        query_filter = [
            TaskTemplateVersion.id.in_(ttvis),
            TaskTemplateVersion.id == Node.task_template_version_id,
            Task.node_id == Node.id,
            TaskInstance.task_id == Task.id,
            TaskInstance.task_resources_id == TaskResources.id,
            TaskResources.queue_id.isnot(None),
        ]
        sql = select(TaskTemplateVersion.id, TaskResources.queue_id).where(
            *query_filter
        )

    rows = session.execute(sql).all()
    column_names = ("id", "queue_id")
    rows = [dict(zip(column_names, ti)) for ti in rows]
    # return a "standard" json format for cli routes
    queue_info = []
    if rows:
        result_dir: Dict = dict()
        for r in rows:
            ttvi = r["id"]
            q = r["queue_id"]
            if ttvi in result_dir.keys():
                if q in result_dir[ttvi].keys():
                    result_dir[ttvi][q] += 1
                else:
                    result_dir[ttvi][q] = 1
            else:
                result_dir[ttvi] = dict()
                result_dir[ttvi][q] = 1
        for ttvi in result_dir.keys():
            # assign to a variable to keep typecheck happy
            max_usage = 0
            for q in result_dir[ttvi].keys():
                if result_dir[ttvi][q] > max_usage:
                    popular_q = q
                    max_usage = result_dir[ttvi][q]
            # get queue name; and return queue id with it
            with session:
                query_filter = [Queue.id == popular_q]
                sql = select(Queue.name).where(*query_filter)
            popular_q_name = session.execute(sql).one()[0]
            queue_info.append(
                {"id": ttvi, "queue": popular_q_name, "queue_id": popular_q}
            )

    resp = jsonify({"queue_info": queue_info})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_template_resource_usage", methods=["POST"])
@cross_origin()
def get_task_template_resource_usage() -> Any:
    """Return the aggregate resource usage for a give TaskTemplate.

    Need to use cross_origin decorator when using the GUI to call a post route.
    This enables Cross Origin Resource Sharing (CORS) on the route. Default is
    most permissive settings.
    """
    data = request.get_json()
    try:
        task_template_version_id = data.pop("task_template_version_id")
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to /task_template_resource_usage", status_code=400
        ) from e

    workflows = data.pop("workflows", None)
    node_args = data.pop("node_args", None)
    ci = data.pop("ci", None)

    session = SessionLocal()
    with session.begin():
        query_filter = [
            TaskTemplateVersion.id == task_template_version_id,
            Task.status == "D",
            TaskInstance.status == "D",
            TaskTemplateVersion.id == Node.task_template_version_id,
            Node.id == Task.node_id,
            Task.id == TaskInstance.task_id,
        ]
        if workflows:
            query_filter += [
                TaskInstance.workflow_run_id == WorkflowRun.id,
                WorkflowRun.workflow_id == Workflow.id,
                Workflow.id.in_(workflows),
            ]
        sql = select(
            TaskInstance.wallclock,
            TaskInstance.maxrss,
            Node.id,
        ).where(*query_filter)
        rows = session.execute(sql).all()
        session.commit()
    column_names = ("r", "m", "node_id")
    rows = [dict(zip(column_names, ti)) for ti in rows]
    result = []
    if rows:
        for r in rows:
            if r["r"] is None:
                r["r"] = 0
            if node_args:
                session = SessionLocal()
                with session.begin():
                    node_f = [NodeArg.arg_id == Arg.id, NodeArg.node_id == r["node_id"]]
                    node_s = select(Arg.name, NodeArg.val).where(*node_f)
                    node_rows = session.execute(node_s).all()
                    session.commit()
                _include = False
                for n in node_rows:
                    if not _include:
                        if n[0] in node_args.keys() and n[1] in node_args[n[0]]:
                            _include = True
                if _include:
                    result.append(r)
            else:
                result.append(r)

    if len(result) == 0:
        resource_usage = SerializeTaskTemplateResourceUsage.to_wire(
            None, None, None, None, None, None, None, None, None, None, None
        )
    else:
        runtimes = []
        mems = []
        for row in result:
            runtimes.append(int(row["r"]))
            mems.append(max(0, 0 if row["m"] is None else int(row["m"])))
        num_tasks = len(runtimes)
        min_mem = int(np.min(mems))
        max_mem = int(np.max(mems))
        mean_mem = round(float(np.mean(mems)), 2)
        min_runtime = int(np.min(runtimes))
        max_runtime = int(np.max(runtimes))
        mean_runtime = round(float(np.mean(runtimes)), 2)
        median_mem = round(float(np.percentile(mems, 50)), 2)
        median_runtime = round(float(np.percentile(runtimes, 50)), 2)

        if ci is None:
            ci_mem = [None, None]
            ci_runtime = [None, None]
        else:
            try:
                ci = float(ci)

                def _calculate_ci(d: List, ci: float) -> List[Any]:
                    interval = st.t.interval(
                        alpha=ci, df=len(d) - 1, loc=np.mean(d), scale=st.sem(d)
                    )
                    return [round(float(interval[0]), 2), round(float(interval[1]), 2)]

                ci_mem = _calculate_ci(mems, ci)
                ci_runtime = _calculate_ci(runtimes, ci)

            except ValueError:
                logger.warn(f"Unable to convert {ci} to float. Use None.")
                ci_mem = [None, None]
                ci_runtime = [None, None]

        resource_usage = SerializeTaskTemplateResourceUsage.to_wire(
            num_tasks,
            min_mem,
            max_mem,
            mean_mem,
            min_runtime,
            max_runtime,
            mean_runtime,
            median_mem,
            median_runtime,
            ci_mem,
            ci_runtime,
        )
    resp = jsonify(resource_usage)
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/workflow_tt_status_viz/<workflow_id>", methods=["GET"])
def get_workflow_tt_status_viz(workflow_id: int) -> Any:
    """Get the status of the workflows for GUI."""
    # return DS
    return_dic: Dict[int, Any] = dict()

    session = SessionLocal()
    with session.begin():
        # For performance reasons, use STRAIGHT_JOIN to set the join order. If not set,
        # the optimizer may choose a suboptimal execution plan for large datasets.
        # Has to be conditional since not all database engines support STRAIGHT_JOIN.
        straight_join = ""
        if SessionLocal.bind.dialect.name == "mysql":
            straight_join = "STRAIGHT_JOIN"

        # join on two columns does not seem to be supported by sqlalchemy
        sql = f"""
              SELECT {straight_join} task_template.id,
                                   task_template.name,
                                   task.id AS id_1,
                                   task.status,
                                   array.max_concurrently_running,
                                   task_template_version.id AS id_2
              FROM task INNER JOIN node ON task.node_id = node.id
                   INNER JOIN task_template_version ON
                         node.task_template_version_id = task_template_version.id
                   INNER JOIN task_template ON
                         task_template_version.task_template_id = task_template.id
                   LEFT OUTER JOIN array ON
                        (array.task_template_version_id = task_template_version.id
                         AND array.workflow_id=task.workflow_id)
              WHERE task.workflow_id = {workflow_id} ORDER BY task.id
              """
        rows = session.execute(sql).all()
        session.commit()

    for r in rows:
        if int(r[0]) in return_dic.keys():
            pass
        else:
            return_dic[int(r[0])] = {
                "id": int(r[0]),
                "name": r[1],
                "tasks": 0,
                "PENDING": 0,
                "SCHEDULED": 0,
                "RUNNING": 0,
                "DONE": 0,
                "FATAL": 0,
                "MAXC": 0,
                "task_template_version_id": int(r[5]),
            }
        return_dic[int(r[0])]["tasks"] += 1
        return_dic[int(r[0])][_cli_label_mapping[r[3]]] += 1
        return_dic[int(r[0])]["MAXC"] = r[4] if r[4] is not None else "NA"
    resp = jsonify(return_dic)
    resp.status_code = 200
    return resp


@blueprint.route("/tt_error_log_viz/<wf_id>/<tt_id>", methods=["GET"])
def get_tt_error_log_viz(tt_id: int, wf_id: int) -> Any:
    """Get the error logs for a task template id for GUI."""
    # return DS
    return_list: List[Any] = []

    session = SessionLocal()
    with session.begin():
        query_filter = [
            TaskTemplateVersion.task_template_id == tt_id,
            Node.task_template_version_id == TaskTemplateVersion.id,
            Task.node_id == Node.id,
            Task.workflow_id == wf_id,
            TaskInstance.task_id == Task.id,
            TaskInstanceErrorLog.task_instance_id == TaskInstance.id,
        ]

        sql = (
            select(
                Task.id,
                TaskInstance.id,
                TaskInstanceErrorLog.id,
                TaskInstanceErrorLog.error_time,
                TaskInstanceErrorLog.description,
            )
            .where(*query_filter)
            .order_by(TaskInstanceErrorLog.id.desc())
        )
        # For performance reasons, use STRAIGHT_JOIN to set the join order. If not set,
        # the optimizer may choose a suboptimal execution plan for large datasets.
        # Has to be conditional since not all database engines support STRAIGHT_JOIN.
        if SessionLocal.bind.dialect.name == "mysql":
            sql = sql.prefix_with("STRAIGHT_JOIN")
        rows = session.execute(sql).all()
        session.commit()
    for r in rows:
        # dict: {<error log id>: [<tid>, <tiid>, <error time>, <error log>}
        return_list.append(
            {
                "task_id": r[0],
                "task_instance_id": r[1],
                "task_instance_err_id": r[2],
                "error_time": r[3],
                "error": r[4],
            }
        )
    resp = jsonify(return_list)
    resp.status_code = 200
    return resp
