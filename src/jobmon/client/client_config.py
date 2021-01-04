from __future__ import annotations
from typing import Optional


from jobmon.config import CLI, ParserDefaults


class ClientConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls) -> ClientConfig:
        cli = CLI()
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)
        ParserDefaults.use_rsyslog(cli.parser)
        ParserDefaults.rsyslog_host(cli.parser)
        ParserDefaults.rsyslog_port(cli.parser)
        ParserDefaults.rsyslog_protocol(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(host=args.web_service_fqdn, port=args.web_service_port,
                   use_rsyslog=args.use_rsyslog, rsyslog_host=args.rsyslog_host,
                   rsyslog_port=args.rsyslog_port, rsyslog_protocol=args.rsyslog_protocol)

    def __init__(self, host: str, port: int, use_rsyslog: bool = False,
                 rsyslog_host: Optional[str] = None, rsyslog_port: Optional[str] = None,
                 rsyslog_protocol: Optional[str] = None):
        self.host = host
        self.port = port
        self.use_rsyslog = use_rsyslog
        self.rsyslog_host = rsyslog_host
        self.rsyslog_port = rsyslog_port
        self.rsyslog_protocol = rsyslog_protocol

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
