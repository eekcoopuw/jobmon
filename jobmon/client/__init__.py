from jobmon.client.client_config import ClientConfig
from jobmon.client.client_logging import ClientLogging
from jobmon.requester import Requester

config = ClientConfig.from_defaults()
shared_requester = Requester(config.url)
ClientLogging.attach_log_handler("JOBMON_SWARM")

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.r_task import RTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.stata_task import StataTask
