"""Configuration specific to reaper."""
from __future__ import annotations

from typing import Any, Optional

from jobmon.config import CLI, ParserDefaults


class WorkflowReaperConfig:
    """Configuration specific to reaper."""

    @classmethod
    def from_defaults(cls: Any) -> WorkflowReaperConfig:
        """If nothing specified by user, set up with defaults."""
        cli = CLI()
        ParserDefaults.reaper_poll_interval_minutes(cli.parser)
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.slack_api_url(cli.parser)
        ParserDefaults.slack_token(cli.parser)
        ParserDefaults.slack_channel_default(cli.parser)
        ParserDefaults.workflow_run_heartbeat_interval(cli.parser)
        ParserDefaults.heartbeat_report_by_buffer(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(
            poll_interval_minutes=args.reaper_poll_interval_minutes,
            host=args.web_service_fqdn,
            port=args.web_service_port,
            slack_api_url=args.slack_api_url,
            slack_token=args.slack_token,
            slack_channel_default=args.slack_channel_default,
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
        )

    def __init__(
        self,
        poll_interval_minutes: int,
        host: str,
        port: str,
        slack_api_url: Optional[str],
        slack_token: Optional[str],
        slack_channel_default: str,
        workflow_run_heartbeat_interval: int,
        heartbeat_report_by_buffer: float,
    ) -> None:
        """Initialization of workflow reaper config."""
        self.poll_interval_minutes = poll_interval_minutes
        self.host = host
        self.port = port
        self.slack_api_url = slack_api_url
        self.slack_token = slack_token
        self.slack_channel_default = slack_channel_default
        self.workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer

    @property
    def url(self) -> str:
        """URL to connect to the jobmon flask web services."""
        return f"http://{self.host}:{self.port}"
