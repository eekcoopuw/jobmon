from jobmon.client._logging import ClientLogging
from jobmon.requests.connection_config import ConnectionConfig
from jobmon.requests.requester import Requester


ClientLogging.attach_log_handler()
logger = ClientLogging.getLogger(__name__)

client_config = ConnectionConfig.from_defaults()
shared_requester = Requester(client_config.url, logger=logger)
