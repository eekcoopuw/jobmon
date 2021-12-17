"""Routes for Clusters."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify

from jobmon.server.web.models import DB
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.routes import finite_state_machine


@finite_state_machine.route("/cluster/<cluster_name>/all_queues", methods=["GET"])
def get_queues_by_cluster_name(cluster_name: str) -> Any:
    """Get the id, name, cluster_name and parameters and connection_parameters of a Cluster."""
    result = (
        DB.session.query(Queue)
        .join(Cluster, Queue.cluster_id == Cluster.id)
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)
        .filter(Cluster.name == cluster_name)
        .all()
    )

    # send back json
    resp = jsonify(queues=[row.to_wire_as_requested_by_client() for row in result])
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/cluster/<cluster_id>/queue/<queue_name>", methods=["GET"])
def get_queue_by_cluster_queue_names(cluster_id: int, queue_name: str) -> Any:
    """Get the id, name, cluster_name and parameters of a Queue.

    Based on cluster_name and queue_name.
    """
    result = (
        DB.session.query(Queue)
        .filter(Queue.cluster_id == cluster_id)
        .filter(Queue.name == queue_name)
        .one_or_none()
    )

    # send back json
    if result is None:
        resp = jsonify(queue=None)
    else:
        resp = jsonify(queue=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
