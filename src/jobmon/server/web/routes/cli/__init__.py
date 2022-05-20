"""Routes used to move through the finite state."""
from flask import Blueprint

from jobmon.server.web import routes

blueprint = Blueprint('cli', __name__)
blueprint.add_url_rule('/', view_func=routes.is_alive, methods=["GET"])
blueprint.add_url_rule('/time', view_func=routes.get_pst_now, methods=["GET"])
blueprint.add_url_rule('/health', view_func=routes.health, methods=["GET"])
blueprint.add_url_rule('/test_bad', view_func=routes.test_route, methods=["GET"])

from jobmon.server.web.routes.cli import (
    array,
    task
)
