"""Routes for Clusters"""
from http import HTTPStatus as StatusCodes

from flask import jsonify

from jobmon.server.web.models import DB

from jobmon.server.web.models.cluster import Cluster

from sqlalchemy.sql import text

from . import jobmon_client


@jobmon_client.route('/all_clusters', methods=['GET'])
def get_clusters():
    """Get the id, cluster_type_name and connection_string of a Cluster."""
    query = """
        SELECT c.id AS cluster_id, ct.name AS cluster_type_name, c.connection_string
        FROM cluster c
            INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
    """
    result = DB.session.query(Cluster).from_statement(text(query)).params(
    ).all()

    # send back json
    resp = jsonify(clusters=[row.to_wire_as_requested_by_client()
                             for row in result])
    resp.status_code = StatusCodes.OK
    return resp


@jobmon_client.route('/cluster/<cluster_name>', methods=['GET'])
def get_cluster_by_name(cluster_name: str):
    """Get the id, cluster_type_name and connection_string of a Cluster."""
    query = """
        SELECT c.id AS cluster_id, ct.name AS cluster_type_name, c.connection_string
        FROM cluster c
            INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
        WHERE
            c.name = :cluster_name
    """
    result = DB.session.query(Cluster).from_statement(text(query)).params(
        cluster_name=cluster_name
    ).one_or_none()

    # send back json
    if result is None:
        resp = jsonify(cluster=None)
    else:
        resp = jsonify(cluster=result.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
