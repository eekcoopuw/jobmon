from jobmon.client.swarm._logging import SwarmLogging
from jobmon.client.requests.connection_config import ConnectionConfig
from jobmon.client.requests.requester import Requester

SwarmLogging.attach_log_handler("JOBMON_SWARM")

client_config = ConnectionConfig.from_defaults()
shared_requester = Requester(client_config.url,
                             logger=SwarmLogging.getLogger(__name__))
