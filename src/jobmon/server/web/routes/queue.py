"""Routes for Clusters."""
from http import HTTPStatus as StatusCodes
from typing import Any

from flask import jsonify
from sqlalchemy import select

from jobmon.server.web.models.queue import Queue
from jobmon.server.web.routes import finite_state_machine, SessionLocal


@finite_state_machine.route("/cluster/<cluster_id>/queue/<queue_name>", methods=["GET"])
def get_queue_by_cluster_queue_names(cluster_id: int, queue_name: str) -> Any:
    """Get the id, name, cluster_name and parameters of a Queue.

    Based on cluster_name and queue_name.
    """
    with SessionLocal.begin() as session:
        select_stmt = select(
            Queue
        ).filter(
            Queue.cluster_id == cluster_id
        ).filter(
            Queue.name == queue_name
        )
        queue = session.execute(select_stmt).scalars().one_or_none()

    # send back json
    if queue is None:
        resp = jsonify(queue=None)
    else:
        resp = jsonify(queue=queue.to_wire_as_requested_by_client())
    resp.status_code = StatusCodes.OK
    return resp
