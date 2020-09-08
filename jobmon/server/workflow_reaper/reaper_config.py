import logging

from jobmon import config
from jobmon.requests.connection_config import ConnectionConfig
from jobmon.requests.requester import Requester

logger = logging.getLogger(__file__)


class WorkflowReaperConfig(object):
    @classmethod
    def from_defaults(cls):
        reaper_config = ConnectionConfig.from_defaults()
        reaper_requester = Requester(reaper_config.url, logger)
        return cls(
            poll_interval_minutes=config.poll_interval_minutes,
            loss_threshold=config.loss_threshold,
            requester=reaper_requester
        )

    def __init__(self, poll_interval_minutes: int, loss_threshold: int, requester: Requester):
        self.poll_interval_minutes = poll_interval_minutes
        self.loss_threshold = loss_threshold
        self.requester = requester

    def __repr__(self):
        return (f"WorkflowReaperConfig(poll_interval_minutes={self.poll_interval_minutes}, "
                f"loss_threshold={self.loss_threshold}, requester_config: "
                f"{self.requester.url})")
