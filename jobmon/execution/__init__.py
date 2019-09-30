from jobmon.execution.scheduler_config import SchedulerConfig
from jobmon.requester import Requester

config = SchedulerConfig.from_defaults()
shared_requester = Requester(config.url)
