from jobmon.client.swarm._logging import SwarmLogging
from jobmon.client.requests.config import ClientConfig
from jobmon.client.requests.requester import Requester

SwarmLogging.attach_log_handler("JOBMON_SWARM")

client_config = ClientConfig.from_defaults()
shared_requester = Requester(client_config.url,
                             logger=SwarmLogging.getLogger(__name__))
