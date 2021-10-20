"""Configuration setting for client-side only."""
from __future__ import annotations

from typing import Optional

from jobmon.config import CLI, ParserDefaults


class ClientConfig(object):
    """This is intended to be a singleton. Any other usage should be done with CAUTION."""

    @classmethod
    def from_defaults(cls) -> ClientConfig:
        """If no special config, set to defaults."""
        cli = CLI()
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.workflow_run_heartbeat_interval(cli.parser)
        ParserDefaults.heartbeat_report_by_buffer(cli.parser)
        ParserDefaults.use_logstash(cli.parser)
        ParserDefaults.logstash_host(cli.parser)
        ParserDefaults.logstash_port(cli.parser)
        ParserDefaults.logstash_protocol(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(host=args.web_service_fqdn, port=args.web_service_port,
                   workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
                   heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
                   use_logstash=args.use_logstash, logstash_host=args.logstash_host,
                   logstash_port=args.logstash_port, logstash_protocol=args.logstash_protocol)

    def __init__(self, host: str, port: int,
                 workflow_run_heartbeat_interval: Optional[int] = None,
                 heartbeat_report_by_buffer: Optional[float] = None,
                 use_logstash: bool = False, logstash_host: Optional[str] = None,
                 logstash_port: Optional[str] = None,
                 logstash_protocol: Optional[str] = None):
        self.host = host
        self.port = port
        self.workflow_run_heartbeat_interval = workflow_run_heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self.use_logstash = use_logstash
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.logstash_protocol = logstash_protocol

    @property
    def url(self):
        """URL to connect to Jobmon."""
        return f"http://{self.host}:{self.port}"
