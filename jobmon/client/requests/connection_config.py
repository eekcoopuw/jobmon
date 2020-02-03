from jobmon import config
from jobmon.client import ClientLogging as logging


logger = logging.getLogger(__file__)


class ConnectionConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls):
        return cls(host=config.jobmon_server_sqdn,
                   port=config.jobmon_service_port)

    def __init__(self, host, port):
        self.host = host
        self.port = port

    @property
    def url(self):
        return "http://{h}:{p}".format(h=self.host, p=self.port)
