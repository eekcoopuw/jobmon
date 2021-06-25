"""Routes for DAGs"""
from http import HTTPStatus as StatusCodes

from flask import current_app as app, jsonify, request

from jobmon.server.web.models import DB
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.edge import Edge
from jobmon.server.web.server_side_exception import InvalidUsage

import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import func, text

from . import jobmon_client


@jobmon_client.route('/dag', methods=['POST'])
def add_dag():
    """Add a new dag to the database.

    Args:
        dag_hash: unique identifier of the dag, included in route
    """
    data = request.get_json()

    # add dag
    dag_hash = data.pop("dag_hash")
    app.logger = app.logger.bind(dag_hash=str(dag_hash))
    app.logger.info(f"Add dag:{dag_hash}")
    try:
        dag = Dag(hash=dag_hash)
        DB.session.add(dag)
        DB.session.commit()

        # return result
        resp = jsonify(dag_id=dag.id, created_date=dag.created_date)
        resp.status_code = StatusCodes.OK

        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM dag
            WHERE hash = :dag_hash
        """
        dag = DB.session.query(Dag).from_statement(text(query)).params(dag_hash=dag_hash).one()
        DB.session.commit()

        # return result
        resp = jsonify(dag_id=dag.id, created_date=dag.created_date)
        resp.status_code = StatusCodes.OK

        return resp


@jobmon_client.route('/dag/<dag_id>/edges', methods=['POST'])
def add_edges(dag_id):
    """Add edges to the edge table."""
    app.logger = app.logger.bind(dag_id=dag_id)
    app.logger.info(f"Add edges for dag {dag_id}")
    try:
        data = request.get_json()
        edges_to_add = data.pop("edges_to_add")
        mark_created = bool(data.pop("mark_created"))
    except KeyError as e:
        raise InvalidUsage(f"{str(e)} in request to {request.path}", status_code=400) from e

    # add dag and cast types
    for edges in edges_to_add:
        edges["dag_id"] = dag_id
        if len(edges['upstream_node_ids']) == 0:
            edges['upstream_node_ids'] = None
        else:
            edges['upstream_node_ids'] = str(edges['upstream_node_ids'])

        if len(edges['downstream_node_ids']) == 0:
            edges['downstream_node_ids'] = None
        else:
            edges['downstream_node_ids'] = str(edges['downstream_node_ids'])

    app.logger.debug(f'Edges: {edges}')

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    edge_insert_stmt = insert(Edge).prefix_with("IGNORE")
    DB.session.execute(edge_insert_stmt, edges_to_add)
    DB.session.commit()

    if mark_created:
        query = """
            SELECT *
            FROM dag
            WHERE id = :dag_id
        """
        dag = DB.session.query(Dag).from_statement(text(query)).params(dag_id=dag_id).one()
        dag.created_date = func.now()
        DB.session.commit()

    # return result
    resp = jsonify()
    resp.status_code = StatusCodes.OK
    return resp
