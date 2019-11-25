from jobmon.client._logging import ClientLogging
from jobmon.client.requests.config import ClientConfig
from jobmon.client.requests.requester import Requester


ClientLogging.attach_log_handler("JOBMON_CLIENT")


client_config = ClientConfig.from_defaults()
shared_requester = Requester(client_config.url,
                             logger=ClientLogging.getLogger(__name__))
