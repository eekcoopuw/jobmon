"""Routes used to move through the finite state."""
from flask import Blueprint

blueprint = Blueprint('cli', __name__)

from jobmon.server.web.routes.cli import (
    array,
    task
)
