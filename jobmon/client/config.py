import os

from jobmon import config
from jobmon.client import ClientLogging as logging

logger = logging.getLogger(__name__)


class InvalidConfig(Exception):
    pass


def derive_jobmon_command_from_env():
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return (
            'singularity run --app jobmon_command {}'
            .format(singularity_img_path).encode())
    return None


class ClientConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls):
        return cls(host=config.jobmon_server_sqdn,
                   port=config.jobmon_service_port,
                   jobmon_command=derive_jobmon_command_from_env(),
                   reconciliation_interval=config.reconciliation_interval,
                   heartbeat_interval=config.heartbeat_interval,
                   report_by_buffer=config.report_by_buffer)

    def __init__(self, host, port, jobmon_command, reconciliation_interval,
                 heartbeat_interval, report_by_buffer):

        self.host = host
        self.port = port
        self.jobmon_command = jobmon_command
        self.reconciliation_interval = reconciliation_interval
        self.heartbeat_interval = heartbeat_interval
        self.report_by_buffer = report_by_buffer

    @property
    def url(self):
        logger.info("http://{h}:{p}".format(h=self.host, p=self.port))
        return "http://{h}:{p}".format(h=self.host, p=self.port)
