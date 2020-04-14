from jobmon import config
from jobmon.client import ClientLogging as logging

logger = logging.getLogger(__file__)


class WorkflowReaperConfig(object):
    @classmethod
    def from_defaults(cls):
        return cls(
            poll_interval=config.poll_interval,
            loss_threshold=config.loss_threshold
        )

    def __init__(self, poll_interval: int, loss_threshold: int):
        self.poll_interval = poll_interval
        self.loss_threshold = loss_threshold
