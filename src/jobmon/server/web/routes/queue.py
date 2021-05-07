"""Routes for Clusters"""
from http import HTTPStatus as StatusCodes

from flask import jsonify

from jobmon.server.web.models import DB
from jobmon.server.web.models.queue import Queue
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType

from . import jobmon_client


@jobmon_client.route('/cluster/<cluster_name>/all_queues', methods=['GET'])
def get_queues_by_cluster_name(cluster_name: str):
    """Get the id, name, cluster_name and parameters and connection_string of a Cluster."""
    result = DB.session.query(Queue)\
        .join(Cluster, Queue.cluster_id == Cluster.id)\
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)\
        .filter(Cluster.name == cluster_name)\
        .all()

    # send back json
    resp = jsonify(queues=[row.to_wire_as_requested_by_client()
                           for row in result])
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/cluster/<cluster_name>/queue/<queue_name>', methods=['GET'])
def get_queue_by_cluster_queue_names(cluster_name: str, queue_name: str):
    """
    Get the id, name, cluster_name and parameters of a Queue based on
    cluster_name and queue_name.
    """
    result = DB.session.query(Queue)\
        .join(Cluster, Queue.cluster_id == Cluster.id)\
        .join(ClusterType, Cluster.cluster_type_id == ClusterType.id)\
        .filter(Cluster.name == cluster_name)\
        .filter(Queue.name == queue_name)\
        .one_or_none()

    # send back json
    if result is None:
        resp = jsonify(queue=None)
    else:
        resp = jsonify(queue=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
