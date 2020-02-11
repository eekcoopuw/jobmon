import os

from typing import Optional

from jobmon import config
from jobmon.client import ClientLogging as logging


logger = logging.getLogger(__file__)


class InvalidConfig(Exception):
    pass


def derive_jobmon_command_from_env() -> Optional[str]:
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return f'singularity run --app jobmon_command {singularity_img_path}'
    return None


class ExecutionConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls):
        return cls(
            jobmon_command=derive_jobmon_command_from_env(),
            workflow_run_heartbeat_interval=config.reconciliation_interval,
            task_heartbeat_interval=config.heartbeat_interval,
            report_by_buffer=config.report_by_buffer,
            n_queued=1000,
            scheduler_poll_interval=10)

    def __init__(self, jobmon_command: Optional[str],
                 workflow_run_heartbeat_interval: int,
                 task_heartbeat_interval: int, report_by_buffer: float,
                 n_queued: int, scheduler_poll_interval: int):
        self.jobmon_command = jobmon_command
        self.workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self.task_heartbeat_interval = task_heartbeat_interval
        self.report_by_buffer = report_by_buffer
        self.n_queued = n_queued
        self.scheduler_poll_interval = scheduler_poll_interval
