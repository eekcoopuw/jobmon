from flask import Blueprint

jobmon_client = Blueprint("jobmon_client", __name__)
jobmon_scheduler = Blueprint("jobmon_scheduler", __name__)
jobmon_swarm = Blueprint("jobmon_swarm", __name__)
jobmon_worker = Blueprint("jobmon_worker", __name__)
jobmon_cli = Blueprint("jobmon_cli", __name__)

from . import jobmon_client_routes, jobmon_scheduler_routes, jobmon_swarm_routes, jobmon_worker_routes, jobmon_cli_routes
from . import dag, node, task, task_instance, task_template, tool, workflow, workflow_run