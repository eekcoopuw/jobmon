from jobmon.client.worker_node._logging import NodeLogging
from jobmon._version import get_versions
__version__ = get_versions()['version']
del get_versions


NodeLogging.attach_log_handler("JOBMON_NODE")
