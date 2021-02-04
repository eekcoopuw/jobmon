from __future__ import annotations

from typing import Optional

from jobmon.config import CLI, ParserDefaults


class WorkflowReaperConfig:

    @classmethod
    def from_defaults(cls) -> WorkflowReaperConfig:
        cli = CLI()
        ParserDefaults.reaper_poll_interval_minutes(cli.parser)
        ParserDefaults.reaper_loss_threshold(cli.parser)
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.slack_api_url(cli.parser)
        ParserDefaults.slack_token(cli.parser)
        ParserDefaults.slack_channel_default(cli.parser)
        ParserDefaults.workflow_run_heartbeat_interval(cli.parser)
        ParserDefaults.task_instance_report_by_buffer(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(poll_interval_minutes=args.reaper_poll_interval_minutes,
                   loss_threshold=args.reaper_loss_threshold,
                   host=args.web_service_fqdn,
                   port=args.web_service_port,
                   slack_api_url=args.slack_api_url,
                   slack_token=args.slack_token,
                   slack_channel_default=args.slack_channel_default,
                   workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
                   task_instance_report_by_buffer=args.task_instance_report_by_buffer)

    def __init__(self, poll_interval_minutes: int, loss_threshold: int, host: str, port: str,
                 slack_api_url: Optional[str], slack_token: Optional[str],
                 slack_channel_default: Optional[str], workflow_run_heartbeat_interval: int,
                 task_instance_report_by_buffer: float) -> None:
        self.poll_interval_minutes = poll_interval_minutes
        self.loss_threshold = loss_threshold
        self.host = host
        self.port = port
        self.slack_api_url = slack_api_url
        self.slack_token = slack_token
        self.slack_channel_default = slack_channel_default
        self.workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self.task_instance_report_by_buffer = task_instance_report_by_buffer

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
