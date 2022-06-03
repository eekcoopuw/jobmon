"""Routes used to move through the finite state."""
from flask import Blueprint
from typing import Optional

from jobmon.server.web import routes
from jobmon.server.web.routes import SessionLocal

blueprint = Blueprint('finite_state_machine', __name__)
blueprint.add_url_rule('/', view_func=routes.is_alive, methods=["GET"])
blueprint.add_url_rule('/time', view_func=routes.get_pst_now, methods=["GET"])
blueprint.add_url_rule('/health', view_func=routes.health, methods=["GET"])
blueprint.add_url_rule('/test_bad', view_func=routes.test_route, methods=["GET"])


@blueprint.teardown_request
def teardown(e: Optional[BaseException]) -> None:
    # remove threadlocal session from registry
    SessionLocal.remove()


from jobmon.server.web.routes.fsm import (
    array,
    dag,
    node,
    task,
    task_instance,
    task_resources,
    task_template,
    tool,
    tool_version,
    workflow,
    workflow_run,
    cluster,
    queue,
)
