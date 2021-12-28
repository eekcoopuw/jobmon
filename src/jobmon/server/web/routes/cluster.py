"""Routes for Clusters."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify

from jobmon.server.web.models import DB
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.routes import finite_state_machine


@finite_state_machine.route("/all_clusters", methods=["GET"])
def get_clusters() -> Any:
    """Get the id, cluster_type_name and connection_parameters of a Cluster."""
    result = (
        DB.session.query(Cluster)
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)
        .all()
    )

    # send back json
    resp = jsonify(clusters=[row.to_wire_as_requested_by_client() for row in result])
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route("/cluster/<cluster_name>", methods=["GET"])
def get_cluster_by_name(cluster_name: str) -> Any:
    """Get the id, cluster_type_name and connection_parameters of a Cluster."""
    result = (
        DB.session.query(Cluster)
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)
        .filter(Cluster.name == cluster_name)
        .one_or_none()
    )

    # send back json
    if result is None:
        resp = jsonify(cluster=None)
    else:
        resp = jsonify(cluster=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
