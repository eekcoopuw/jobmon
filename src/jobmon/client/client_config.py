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
        ParserDefaults.use_logstash(cli.parser)
        ParserDefaults.logstash_host(cli.parser)
        ParserDefaults.logstash_port(cli.parser)
        ParserDefaults.logstash_protocol(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(host=args.web_service_fqdn, port=args.web_service_port,
                   use_logstash=args.use_logstash, logstash_host=args.logstash_host,
                   logstash_port=args.logstash_port, logstash_protocol=args.logstash_protocol)

    def __init__(self, host: str, port: int, use_logstash: bool = False,
                 logstash_host: Optional[str] = None, logstash_port: Optional[str] = None,
                 logstash_protocol: Optional[str] = None):
        self.host = host
        self.port = port
        self.use_logstash = use_logstash
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.logstash_protocol = logstash_protocol

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
