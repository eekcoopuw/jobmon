from jobmon.client._logging import ClientLogging


ClientLogging.attach_log_handler()
logger = ClientLogging.getLogger(__name__)
