"""Routes for Clusters"""
from http import HTTPStatus as StatusCodes

from flask import jsonify

from jobmon.server.web.models import DB
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType

from . import jobmon_client


@jobmon_client.route('/all_clusters', methods=['GET'])
def get_clusters():
    """Get the id, cluster_type_name and connection_string of a Cluster."""
    result = DB.session.query(Cluster)\
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)\
        .all()

    # send back json
    resp = jsonify(clusters=[row.to_wire_as_requested_by_client()
                             for row in result])
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/cluster/<cluster_name>', methods=['GET'])
def get_cluster_by_name(cluster_name: str):
    """Get the id, cluster_type_name and connection_string of a Cluster."""
    result = DB.session.query(Cluster)\
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)\
        .filter(Cluster.name==cluster_name)\
        .one_or_none()

    # send back json
    if result is None:
        resp = jsonify(cluster=None)
    else:
        resp = jsonify(cluster=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
