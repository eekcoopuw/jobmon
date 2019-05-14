from jobmon.client.config import ClientConfig
from jobmon.client.requester import Requester

client_config = ClientConfig.from_defaults()
shared_requester = Requester(client_config.jm_conn)
