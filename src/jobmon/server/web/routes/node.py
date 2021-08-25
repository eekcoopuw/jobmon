"""Routes used by the main jobmon client."""
from http import HTTPStatus as StatusCodes
import json
from typing import Any

from flask import current_app as app, jsonify, request
from jobmon.server.web.models import DB
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import text

from jobmon.server.web.routes import finite_state_machine


@finite_state_machine.route('/node', methods=['GET'])
def get_node_id() -> Any:
    """Get a node id: If a matching node isn't found, return None.

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a node
        task_template_version_id: version id of the task_template a node belongs to.
    """
    query = """
        SELECT node.id
        FROM node
        WHERE
            node_args_hash = :node_args_hash
            AND task_template_version_id = :task_template_version_id"""
    result = DB.session.query(Node).from_statement(text(query)).params(
        node_args_hash=request.args['node_args_hash'],
        task_template_version_id=request.args['task_template_version_id']
    ).one_or_none()

    if result is None:
        resp = jsonify({'node_id': None})
    else:
        resp = jsonify({'node_id': result.id})
    resp.status_code = StatusCodes.OK
    return resp


@finite_state_machine.route('/node', methods=['POST'])
def add_node() -> Any:
    """Add a new node to the database.

    Args:
        node_args_hash: unique identifier of all NodeArgs associated with a node.
        task_template_version_id: version id of the task_template a node belongs to.
        node_args: key-value pairs of arg_id and a value.
    """
    data = request.get_json()
    app.logger = app.logger.bind(
        task_template_version_id=data['task_template_version_id'],
        node_args_hash=str(data['node_args_hash'])
    )
    app.logger.info(f"Add node with ttv id:{data['task_template_version_id']}, "
                    f"node_args_hash {data['node_args_hash']}")
    # add node
    try:
        node = Node(task_template_version_id=data['task_template_version_id'],
                    node_args_hash=data['node_args_hash'])
        DB.session.add(node)
        DB.session.commit()

        # lock for insert to related tables
        DB.session.refresh(node, with_for_update=True)

        # add node_args
        node_args = json.loads(data['node_args'])
        for arg_id, value in node_args.items():
            app.logger.info(
                f'Adding node_arg with node_id: {node.id}, arg_id: {arg_id}, and val: {value}'
            )
            node_arg = NodeArg(node_id=node.id, arg_id=arg_id, val=value)
            DB.session.add(node_arg)
        DB.session.commit()
        # return result
        resp = jsonify(node_id=node.id)
        resp.status_code = StatusCodes.OK
        return resp
    except sqlalchemy.exc.IntegrityError:
        DB.session.rollback()
        query = """
            SELECT *
            FROM node
            WHERE
                task_template_version_id = :task_template_version_id
                AND node_args_hash = :node_args_hash
        """
        node = DB.session.query(Node).from_statement(text(query)).params(
            task_template_version_id=data['task_template_version_id'],
            node_args_hash=data['node_args_hash']
        ).one()
        DB.session.commit()
        # return result
        resp = jsonify(node_id=node.id)
        resp.status_code = StatusCodes.OK
        return resp


@finite_state_machine.route('/nodes', methods=['POST'])
def add_nodes() -> Any:
    """Add a chunk of nodes to the database.

    Args:
        nodes: a list of
            node_args_hash: unique identifier of all NodeArgs associated with a node.
        task_template_version_id: version id of the task_template a node belongs to.
        node_args: key-value pairs of arg_id and a value.
    """
    data = request.get_json()
    # Extract node and node_args
    nodes = [(n['task_template_version_id'], n['node_args_hash']) for n in data['nodes']]

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    nodes_to_add = [
        {'task_template_version_id': ttv, 'node_args_hash': arghash}
        for ttv, arghash in nodes
    ]
    node_insert_stmt = insert(Node).prefix_with("IGNORE")
    DB.session.execute(node_insert_stmt, nodes_to_add)
    DB.session.commit()

    # Retrieve the node IDs
    ttvids, node_arg_hashes = zip(*nodes)
    node_ids_query = """
        SELECT *
        FROM node
        WHERE
            task_template_version_id IN :task_template_version_id
            AND node_args_hash IN :node_args_hash
    """
    node_ids = DB.session.query(Node).from_statement(text(node_ids_query)).params(
        task_template_version_id=ttvids,
        node_args_hash=node_arg_hashes
    ).all()

    node_id_dict = {
        (n.task_template_version_id, str(n.node_args_hash)): n.id
        for n in node_ids
    }

    # Add node args. Cast hash to string to match DB schema
    node_args = {(n['task_template_version_id'], str(n['node_args_hash'])): n['node_args']
                 for n in data['nodes']}

    node_args_list = []
    for node_id_tuple, arg in node_args.items():

        node_id = node_id_dict[node_id_tuple]

        for arg_id, val in arg.items():
            app.logger.debug(f'Adding node_arg with node_id: {node_id}, arg_id: {arg_id}, '
                             f'and val: {val}',
                             node_id=node_id)
            node_args_list.append({
                'node_id': node_id,
                'arg_id': arg_id,
                'val': val
            })

    # Bulk insert again with raw SQL
    if node_args_list:
        node_arg_insert_stmt = insert(NodeArg).prefix_with("IGNORE")
        DB.session.execute(node_arg_insert_stmt, node_args_list)
        DB.session.commit()

    # return result
    return_nodes = {':'.join(str(i) for i in key): val for key, val in
                    node_id_dict.items()}
    resp = jsonify(nodes=return_nodes)
    resp.status_code = StatusCodes.OK
    return resp
