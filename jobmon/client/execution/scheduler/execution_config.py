import logging
import os

from jobmon import config


logger = logging.getLogger(__file__)


class InvalidConfig(Exception):
    pass


def derive_jobmon_command_from_env():
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return (
            'singularity run --app jobmon_command {}'
            .format(singularity_img_path).encode())
    return None


class ExecutionConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls):
        return cls(jobmon_command=derive_jobmon_command_from_env(),
                   reconciliation_interval=config.reconciliation_interval,
                   heartbeat_interval=config.heartbeat_interval,
                   report_by_buffer=config.report_by_buffer,
                   n_queued=1000)

    def __init__(self, jobmon_command, reconciliation_interval,
                 heartbeat_interval, report_by_buffer, n_queued):
        self.jobmon_command = jobmon_command
        self.reconciliation_interval = reconciliation_interval
        self.heartbeat_interval = heartbeat_interval
        self.report_by_buffer = report_by_buffer
        self.n_queued = n_queued
