"""Routes for TaskTemplate."""
from http import HTTPStatus as StatusCodes
import json
import numpy as np
from typing import Any, cast, Dict, List, Set

from flask import jsonify, request
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session
import structlog

from jobmon.server.web.models.node import Node
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.routes.cli import blueprint

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
            query_filter = [Task.id == t_id,
                      Task.node_id == Node.id,
                      Node.task_template_version_id == TaskTemplateVersion.id,
                      TaskTemplateVersion.task_template_id == TaskTemplate.id]
            sql = (
                select(TaskTemplateVersion.id,
                       TaskTemplate.name,
                      )
                .where(*query_filter)
            )

        else:
            query_filter = [Task.workflow_id == wf_id,
                            Task.node_id == Node.id,
                            Node.task_template_version_id == TaskTemplateVersion.id,
                            TaskTemplateVersion.task_template_id == TaskTemplate.id]
            sql = (
                select(TaskTemplateVersion.id,
                       TaskTemplate.name,
                       )
                .where(*query_filter)
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
    ttvis = [int(i) for i in ttvis[1:-1].split(",")]
    # null core should be treated as 1 instead of 0
    session = SessionLocal()
    with session.begin():
        query_filter = [TaskTemplateVersion.id.in_(ttvis),
            TaskTemplateVersion.id == Node.task_template_version_id,
            Task.node_id == Node.id,
            Task.task_resources_id == TaskResources.id]

        sql = (
            select(
                TaskTemplateVersion.id,
                TaskResources.requested_resources
            ).where(*query_filter)
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
    sql = f"""
            SELECT task_template_version.id as id, task_resources.queue_id as queue_id
            FROM task, node, task_template_version, task_resources, task_instance
            WHERE task_template_version.id in {ttvis}
            AND node.task_template_version_id=task_template_version.id
            AND task.node_id=node.id
            AND task_instance.task_id=task.id
            AND task_instance.task_resources_id = task_resources.id
            AND task_resources.queue_id is not null
    """
    rows = DB.session.execute(sql).fetchall()
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
            sql = f"SELECT name FROM queue WHERE id={popular_q}"
            popular_q_name = DB.session.execute(sql).fetchone()["name"]
            queue_info.append(
                {"id": ttvi, "queue": popular_q_name, "queue_id": popular_q}
            )

    resp = jsonify({"queue_info": queue_info})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/task_template_resource_usage", methods=["POST"])
def get_task_template_resource_usage() -> Any:
    """Return the aggregate resource usage for a give TaskTemplate."""
    data = request.get_json()
    try:
        task_template_version_id = data.pop("task_template_version_id")
    except Exception as e:
        raise InvalidUsage(
            f"{str(e)} in request to /task_template_resource_usage", status_code=400
        ) from e

    params = {"task_template_version_id": task_template_version_id}
    where_clause = ""
    from_clause = ""
    workflows = data.pop("workflows", None)
    node_args = data.pop("node_args", None)
    ci = data.pop("ci", None)
    if workflows:
        from_clause += ", workflow_run AS wfr, workflow AS wf"
        where_clause += (
            " AND ti.workflow_run_id = wfr.id AND wfr.workflow_id = wf.id AND "
            "wf.id IN :workflows"
        )
        params["workflows"] = workflows

    if node_args:
        from_clause += ", arg AS a, node_arg AS na"
        where_clause += " AND t.node_id = na.node_id AND a.id = na.arg_id AND ("

        def construct_arg_list_clause(k: str, v: list) -> str:
            quoted_arg_vals = ",".join(f"'{x}'" for x in v)
            return f"(a.name = '{k}' AND na.val IN ({quoted_arg_vals}))"

        where_clause += " OR ".join(
            construct_arg_list_clause(key, value) for key, value in node_args.items()
        )
        where_clause += ")"

    query = """
              SELECT
                    CASE
                        WHEN ti.wallclock is Null THEN 0
                        ELSE ti.wallclock
                    END AS r,
                    CASE
                        WHEN ti.maxpss is Null AND ti.maxrss is Null THEN 0
                        WHEN ti.maxpss is Null AND ti.maxrss is not Null THEN ti.maxrss
                        WHEN ti.maxpss is NOT Null AND ti.maxrss is Null THEN ti.maxpss
                        WHEN ti.maxpss > ti.maxrss THEN ti.maxpss
                        ELSE ti.maxrss
                     END AS m
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
    """.format(
        from_clause=from_clause, where_clause=where_clause
    )
    result = DB.session.execute(query, params).fetchall()
    if result is None or len(result) == 0:
        resource_usage = SerializeTaskTemplateResourceUsage.to_wire(
            None, None, None, None, None, None, None, None, None, None, None
        )
    else:
        runtimes = []
        mems = []
        for row in result:
            runtimes.append(int(row["r"]))
            mems.append(max(0, int(row["m"])))
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

                def _calculate_ci(d: List, ci: float) -> List[float]:
                    interval = st.t.interval(
                        alpha=ci, df=len(d) - 1, loc=np.mean(d), scale=st.sem(d)
                    )
                    return [round(float(interval[0]), 2), round(float(interval[1]), 2)]

                ci_mem = _calculate_ci(mems, ci)
                ci_runtime = _calculate_ci(runtimes, ci)

            except ValueError:
                logger.warn(f"Unable to convert {ci} to float. Use NaN.")
                ci_mem = [float("nan"), float("nan")]
                ci_runtime = [float("nan"), float("nan")]

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
