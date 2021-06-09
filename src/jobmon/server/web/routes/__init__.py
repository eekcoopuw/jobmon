from flask import Blueprint

jobmon_cli = Blueprint("jobmon_cli", __name__)
jobmon_client = Blueprint("jobmon_client", __name__)
jobmon_distributor = Blueprint("jobmon_distributor", __name__)
jobmon_swarm = Blueprint("jobmon_swarm", __name__)
jobmon_worker = Blueprint("jobmon_worker", __name__)

from jobmon.server.web.routes.blueprints import cli_routes
from jobmon.server.web.routes.blueprints import client_routes
from jobmon.server.web.routes.blueprints import distributor_routes
from jobmon.server.web.routes.blueprints import swarm_routes
from jobmon.server.web.routes.blueprints import worker_routes

from jobmon.server.web.routes import (dag, node, task, task_instance, task_template, tool,
                                      tool_version, workflow, workflow_run, cluster_type,
                                      cluster, queue)
