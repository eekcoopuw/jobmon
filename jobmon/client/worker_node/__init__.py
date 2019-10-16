from jobmon.client.client_logging import ClientLogging
from jobmon._version import get_versions
__version__ = get_versions()['version']
del get_versions

RELEASE_NAME = "fruitbat"

ClientLogging.attach_log_handler("JOBMON_NODE Michelle")
