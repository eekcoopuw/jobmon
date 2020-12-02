from __future__ import annotations


from jobmon.config import CLI, ParserDefaults
from jobmon.client import ClientLogging as logging


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

    def __init__(self, host: str, port: int, use_rsyslog: bool = None,
                 rsyslog_host: str = None, rsyslog_port: str = None,
                 rsyslog_protocol: str = None):
        self.host = host
        self.port = port
        self.use_rsyslog = use_rsyslog
        self.rsyslog_host = rsyslog_host
        self.rsyslog_port = rsyslog_port
        self.rsyslog_protocol = rsyslog_protocol
        logger = logging.getLogger(__name__, self.use_rsyslog, self.rsyslog_host,
                                   self.rsyslog_port, self.rsyslog_protocol)

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
