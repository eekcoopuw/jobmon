from flask import Blueprint

jobmon_client = Blueprint("jobmon_client", __name__)
jobmon_scheduler = Blueprint("jobmon_scheduler", __name__)
jobmon_swarm = Blueprint("jobmon_swarm", __name__)
jobmon_worker = Blueprint("jobmon_worker", __name__)
jobmon_cli = Blueprint("jobmon_cli", __name__)

from . import jobmon_client_2, jobmon_scheduler_2, jobmon_swarm_2, jobmon_worker_2, jobmon_cli_2
