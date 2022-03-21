"""Configuration setting for client-side only."""
from __future__ import annotations

from contextlib import redirect_stderr
import io
from typing import Any, Optional

from jobmon.config import CLI, install_default_config_from_plugin, ParserDefaults


class ClientConfig(object):
    """This is intended to be a singleton. Any other usage should be done with CAUTION."""

    @classmethod
    def from_defaults(cls: Any) -> ClientConfig:
        """If no special config, set to defaults."""
        cli = CLI()
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.workflow_run_heartbeat_interval(cli.parser)
        ParserDefaults.heartbeat_report_by_buffer(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        try:
            with redirect_stderr(io.StringIO()):
                args = cli.parse_args("")
        except SystemExit:
            args = install_default_config_from_plugin(cli)

        return cls(
            host=args.web_service_fqdn,
            port=args.web_service_port,
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
        )

    def __init__(
        self,
        host: str,
        port: int,
        workflow_run_heartbeat_interval: Optional[int] = None,
        heartbeat_report_by_buffer: Optional[float] = None,
    ) -> None:
        """Initialization of ClientConfig."""
        self.host = host
        self.port = port
        self.workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer

    @property
    def url(self) -> str:
        """URL to connect to Jobmon."""
        return f"http://{self.host}:{self.port}"
