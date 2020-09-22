from __future__ import annotations

from typing import Optional

from jobmon.config import CLI, ParserDefaults


class WorkflowReaperConfig:

    @classmethod
    def from_defaults(cls) -> WorkflowReaperConfig:
        cli = CLI()
        ParserDefaults.poll_interval_minutes(cli.parser)
        ParserDefaults.loss_threshold(cli.parser)
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.slack_api_url(cli.parser)
        ParserDefaults.slack_token(cli.parser)
        ParserDefaults.slack_channel_default(cli.parser)

        args = cli.parse_args()

        return cls(poll_interval_minutes=args.poll_interval_minutes,
                   loss_threshold=args.loss_threshold,
                   host=args.web_service_fqdn,
                   port=args.web_service_port,
                   slack_api_url=args.slack_api_url,
                   slack_token=args.slack_token,
                   slack_channel_default=args.slack_channel_default)

    def __init__(self, poll_interval_minutes: int, loss_threshold: int, host: str, port: str,
                 slack_api_url: Optional[str], slack_token: Optional[str],
                 slack_channel_default: Optional[str]) -> None:
        self.poll_interval_minutes = poll_interval_minutes
        self.loss_threshold = loss_threshold
        self.host = host
        self.port = port
        self.slack_api_url = slack_api_url
        self.slack_token = slack_token
        self.slack_channel_default = slack_channel_default

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
