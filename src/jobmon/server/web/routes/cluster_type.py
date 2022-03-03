"""Routes for Clusters."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify

from jobmon.server.web.models import DB
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.routes import finite_state_machine


@finite_state_machine.route("/cluster_type/<cluster_type_name>", methods=["GET"])
def get_cluster_type_by_name(cluster_type_name: str) -> Any:
    """Get the id, name and package_location of a ClusterType."""
    result = (
        DB.session.query(ClusterType)
        .filter(ClusterType.name == cluster_type_name)
        .one_or_none()
    )

    # send back json
    if result is None:
        resp = jsonify(cluster_type=None)
    else:
        resp = jsonify(cluster_type=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/cluster_type/task_id/<task_id>", methods=["GET"])
def get_cluster_type_by_task_id(task_id: str) -> Any:
    """Get the cluster_type_id by task id."""
    sql = """SELECT DISTINCT cluster_type.id as cluster_type_id
            FROM cluster_type, queue, task_resources, cluster, task
            WHERE task.id={}
            AND task.task_resources_id=task_resources.id
            AND queue.id = task_resources.queue_id
            AND queue.cluster_id = cluster.id
            AND cluster_type.id = cluster.cluster_type_id""".format(
        task_id
    )

    result = DB.session.execute(sql).fetchone()
    # send back json
    if result is None:
        resp = jsonify(cluster_type_id=None)
    else:
        resp = jsonify(cluster_type_id=result["cluster_type_id"])
    resp.status_code = StatusCodes.OK
    return resp
