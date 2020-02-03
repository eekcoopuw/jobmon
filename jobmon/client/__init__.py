from jobmon.client._logging import ClientLogging
from jobmon.client.requests.connection_config import ConnectionConfig
from jobmon.client.requests.requester import Requester


ClientLogging.attach_log_handler()


client_config = ConnectionConfig.from_defaults()
shared_requester = Requester(client_config.url)
