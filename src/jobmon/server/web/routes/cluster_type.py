"""Routes for Clusters."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from jobmon.server.web.models import DB
from jobmon.server.web.models.cluster_type import ClusterType

from . import jobmon_client


@jobmon_client.route('/cluster_type/<cluster_type_name>', methods=['GET'])
def get_cluster_type_by_name(cluster_type_name: str) -> Any:
    """Get the id, name and package_location of a ClusterType."""
    result = DB.session.query(ClusterType)\
        .filter(ClusterType.name == cluster_type_name)\
        .one_or_none()

    # send back json
    if result is None:
        resp = jsonify(cluster_type=None)
    else:
        resp = jsonify(cluster_type=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
