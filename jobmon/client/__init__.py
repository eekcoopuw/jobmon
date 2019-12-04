from jobmon.client._logging import ClientLogging
from jobmon.client.requests.config import ClientConfig
from jobmon.client.requests.requester import Requester


<<<<<<< HEAD
ClientLogging.attach_log_handler("JOBMON_CLIENT")


client_config = ClientConfig.from_defaults()
shared_requester = Requester(client_config.url,
                             logger=ClientLogging.getLogger(__name__))
=======
# from jobmon.client.swarm.workflow.workflow import Workflow
# from jobmon.client.swarm.workflow.bash_task import BashTask
# from jobmon.client.swarm.workflow.r_task import RTask
# from jobmon.client.swarm.workflow.python_task import PythonTask
# from jobmon.client.swarm.workflow.stata_task import StataTask
>>>>>>> f5c68387530f702a91f8ed26b22e9a5c14f26960
