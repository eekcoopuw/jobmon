from typing import Optional

from jobmon.client.execution.strategies.api import get_scheduling_executor_by_name
from jobmon.client.execution.strategies.base import Executor
from jobmon.client.execution.scheduler.task_instance_scheduler import TaskInstanceScheduler
from jobmon.client.execution.scheduler.scheduler_config import SchedulerConfig
from jobmon.log_config import configure_logger, get_logstash_handler_config
from jobmon.requester import Requester


def get_scheduler(workflow_id: int, workflow_run_id: int,
                  scheduler_config: Optional[SchedulerConfig] = None,
                  executor: Optional[Executor] = None,
                  executor_class: Optional[str] = 'SGEExecutor',
                  *args, **kwargs) -> TaskInstanceScheduler:
    if scheduler_config is None:
        scheduler_config = SchedulerConfig.from_defaults()
    if scheduler_config.use_logstash:
        syslog_config = get_logstash_handler_config(
            logstash_host=scheduler_config.logstash_host,
            logstash_port=scheduler_config.logstash_port,
            logstash_protocol=scheduler_config.rsyslog_protocol
        )
    else:
        syslog_config = None
    configure_logger("jobmon.client.execution", syslog_config)

    # TODO: make the default executor configurable
    if executor is None:
        executor = get_scheduling_executor_by_name(executor_class, *args, **kwargs)

    requester = Requester(scheduler_config.url)
    scheduler = TaskInstanceScheduler(
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        executor=executor,
        requester=requester,
        workflow_run_heartbeat_interval=scheduler_config.workflow_run_heartbeat_interval,
        task_heartbeat_interval=scheduler_config.task_heartbeat_interval,
        report_by_buffer=scheduler_config.report_by_buffer,
        n_queued=scheduler_config.n_queued,
        scheduler_poll_interval=scheduler_config.scheduler_poll_interval,
        jobmon_command=scheduler_config.jobmon_command
    )
    return scheduler
