"""QPID configuration for qpid specifics only."""
from __future__ import annotations

from typing import Any

from jobmon.config import CLI, ParserDefaults


class UsageConfig:
    """QPID specific configuration."""

    @classmethod
    def from_defaults(cls: Any) -> UsageConfig:
        """Set up config from defaults if no alternates specified."""
        cli = CLI()
        ParserDefaults.db_host(cli.parser)
        ParserDefaults.db_port(cli.parser)
        ParserDefaults.db_user(cli.parser)
        ParserDefaults.db_pass(cli.parser)
        ParserDefaults.db_name(cli.parser)
        ParserDefaults.qpid_cluster(cli.parser)
        ParserDefaults.qpid_uri(cli.parser)
        # squid new config
        ParserDefaults.squid_polling_interval(cli.parser)
        ParserDefaults.squid_max_update_per_second(cli.parser)
        ParserDefaults.squid_cluster(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(
            db_host=args.db_host,
            db_port=args.db_port,
            db_user=args.db_user,
            db_pass=args.db_pass,
            db_name=args.db_name,
            qpid_cluster=args.qpid_cluster,
            qpid_uri=args.qpid_uri,
            # squid
            squid_polling_interval=args.squid_polling_interval,
            squid_max_update_per_second=args.squid_max_update_per_second,
            squid_cluster=args.squid_cluster,
        )

    def __init__(
        self,
        db_host: str,
        db_port: str,
        db_user: str,
        db_pass: str,
        db_name: str,
        qpid_cluster: str,
        qpid_uri: str,
        # squid
        squid_polling_interval: int,
        squid_max_update_per_second: int,
        squid_cluster: str,
    ) -> None:
        """Initialization of the QPID configuration."""
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.qpid_cluster = qpid_cluster
        self.qpid_uri = qpid_uri
        # squid
        self.squid_polling_interval = squid_polling_interval
        self.squid_max_update_per_second = squid_max_update_per_second
        self.squid_cluster = squid_cluster

    @property
    def conn_str(self) -> str:
        """Connection string to connect to the jobmon database."""
        conn_str = "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=self.db_user,
            pw=self.db_pass,
            host=self.db_host,
            port=self.db_port,
            db=self.db_name,
        )
        return conn_str

    @property
    def qpid_uri_base(self) -> str:
        """The uri prefix to jobmaxpss API."""
        qpid_api_url = f"{self.qpid_uri}/{self.qpid_cluster}/jobmaxpss"
        return qpid_api_url
