"""Routes used by the main jobmon client."""

from http import HTTPStatus as StatusCodes
from typing import Any, cast, Dict

from fastapi import Request
from sqlalchemy import insert, select
from starlette.responses import JSONResponse
import structlog

from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db_admin import get_session_local
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.routes.v1.fsm import fsm_router as api_v1_router
from jobmon.server.web.routes.v2.fsm import fsm_router as api_v2_router

logger = structlog.get_logger(__name__)
SessionLocal = get_session_local()
_CONFIG = get_jobmon_config()


@api_v1_router.post("/nodes")
@api_v2_router.post("/nodes")
async def add_nodes(request: Request) -> Any:
    """Add a chunk of nodes to the database.

    Args:
        request: The request object.
    """
    data = cast(Dict, await request.json())
    # Extract node and node_args

    # Bulk insert the nodes and node args with raw SQL, for performance. Ignore duplicate
    # keys
    with SessionLocal() as session:
        with session.begin():
            node_keys = [
                (n["task_template_version_id"], n["node_args_hash"])
                for n in data["nodes"]
            ]
            node_insert_stmt = insert(Node).values(
                [
                    {"task_template_version_id": ttv, "node_args_hash": arghash}
                    for ttv, arghash in node_keys
                ]
            )
            if SessionLocal and "mysql" in _CONFIG.get("db", "sqlalchemy_database_uri"):
                node_insert_stmt = node_insert_stmt.prefix_with("IGNORE")
            if SessionLocal and "sqlite" in _CONFIG.get(
                    "db", "sqlalchemy_database_uri"
            ):
                node_insert_stmt = node_insert_stmt.prefix_with("OR IGNORE")

            session.execute(node_insert_stmt)
            session.flush()

        # Retrieve the node IDs
        ttvids, node_arg_hashes = zip(*node_keys)
        select_stmt = select(Node).where(
            Node.task_template_version_id.in_(ttvids),
            Node.node_args_hash.in_(node_arg_hashes),
        )
        nodes = session.execute(select_stmt).scalars().all()

        node_id_dict = {
            (n.task_template_version_id, n.node_args_hash): n.id for n in nodes
        }

        # Add node args. Cast hash to string to match DB schema
        node_args = {
            (n["task_template_version_id"], n["node_args_hash"]): n["node_args"]
            for n in data["nodes"]
        }

        node_args_list = []
        for node_id_tuple, arg in node_args.items():
            node_id = node_id_dict[node_id_tuple]
            local_logger = logger.bind(node_id=node_id)

            for arg_id, val in arg.items():
                local_logger.debug(
                    "Adding node_arg", node_id=node_id, arg_id=arg_id, val=val
                )
                node_args_list.append(
                    {"node_id": node_id, "arg_id": arg_id, "val": val}
                )

    # Bulk insert again with raw SQL. Separate method for separate session.
    _insert_node_args(node_args_list)

    # return result
    return_nodes = {
        ":".join(str(i) for i in key): val for key, val in node_id_dict.items()
    }
    resp = JSONResponse(content={"nodes": return_nodes}, status_code=StatusCodes.OK)
    return resp


def _insert_node_args(node_args_list: list) -> None:
    with SessionLocal() as session:
        with session.begin():
            if node_args_list:
                node_arg_insert_stmt = insert(NodeArg).values(node_args_list)
                if SessionLocal and "mysql" in _CONFIG.get(
                        "db", "sqlalchemy_database_uri"
                ):
                    node_arg_insert_stmt = node_arg_insert_stmt.prefix_with("IGNORE")
                if SessionLocal and "sqlite" in _CONFIG.get(
                        "db", "sqlalchemy_database_uri"
                ):
                    node_arg_insert_stmt = node_arg_insert_stmt.prefix_with("OR IGNORE")

                session.execute(node_arg_insert_stmt)
                ()
