from typing import Optional

from jobmon.config import CLI, ParserDefaults


class SchedulerConfig:

    @classmethod
    def from_defaults(cls):
        cli = CLI()

        ParserDefaults.worker_node_entry_point(cli.parser)
        ParserDefaults.workflow_run_heartbeat_interval(cli.parser)
        ParserDefaults.task_instance_heartbeat_interval(cli.parser)
        ParserDefaults.task_instance_report_by_buffer(cli.parser)
        ParserDefaults.scheduler_n_queued(cli.parser)
        ParserDefaults.scheduler_poll_interval(cli.parser)
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.use_rsyslog(cli.parser)
        ParserDefaults.rsyslog_host(cli.parser)
        ParserDefaults.rsyslog_port(cli.parser)
        ParserDefaults.rsyslog_protocol(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(
            jobmon_command=args.worker_node_entry_point,
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            task_heartbeat_interval=args.task_instance_heartbeat_interval,
            report_by_buffer=args.task_instance_report_by_buffer,
            n_queued=args.scheduler_n_queued,
            scheduler_poll_interval=args.scheduler_poll_interval,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port,
            use_rsyslog=args.use_rsyslog,
            rsyslog_host=args.rsyslog_host,
            rsyslog_port=args.rsyslog_port,
            rsyslog_protocol=args.rsyslog_protocol
        )

    def __init__(self, workflow_run_heartbeat_interval: int, task_heartbeat_interval: int,
                 report_by_buffer: float, n_queued: int, scheduler_poll_interval: int,
                 web_service_fqdn: str, web_service_port: str,
                 jobmon_command: Optional[str] = None, use_rsyslog: bool = False,
                 rsyslog_host: str = "", rsyslog_port: str = "", rsyslog_protocol: str = ""):
        self.jobmon_command = jobmon_command
        self.workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self.task_heartbeat_interval = task_heartbeat_interval
        self.report_by_buffer = report_by_buffer
        self.n_queued = n_queued
        self.scheduler_poll_interval = scheduler_poll_interval
        self.web_service_fqdn = web_service_fqdn
        self.web_service_port = web_service_port
        self.use_rsyslog = use_rsyslog
        self.rsyslog_host = rsyslog_host
        self.rsyslog_port = rsyslog_port
        self.rsyslog_protocol = rsyslog_protocol

    @property
    def url(self):
        return f"http://{self._web_service_fqdn}:{self._web_service_port}"
