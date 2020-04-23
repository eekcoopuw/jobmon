from jobmon import config
from jobmon.client import ClientLogging as logging

logger = logging.getLogger(__file__)


class WorkflowReaperConfig(object):
    @classmethod
    def from_defaults(cls):
        return cls(
            poll_interval_minutes=config.poll_interval_minutes,
            loss_threshold=config.loss_threshold
        )

    def __init__(self, poll_interval_minutes: int, loss_threshold: int):
        self.poll_interval_minutes = poll_interval_minutes
        self.loss_threshold = loss_threshold
