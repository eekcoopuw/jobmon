from __future__ import annotations


from jobmon.config import CLI, ParserDefaults
from jobmon.client import ClientLogging as logging


logger = logging.getLogger(__name__)


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

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(host=args.web_service_fqdn, port=args.web_service_port)

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
