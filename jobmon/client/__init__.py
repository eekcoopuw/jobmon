from jobmon.client.config import ClientConfig
from jobmon.client.requester import Requester
from jobmon.client.client_logging import ClientLogging

client_config = ClientConfig.from_defaults()
shared_requester = Requester(client_config.url)

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.r_task import RTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.stata_task import StataTask


ClientLogging.attach_log_handler("JOBMON_CLIENT")
