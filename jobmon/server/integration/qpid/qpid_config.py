from __future__ import annotations

from jobmon.config import CLI, ParserDefaults


class QPIDConfig:

    @classmethod
    def from_defaults(cls) -> QPIDConfig:
        cli = CLI()
        ParserDefaults.db_host(cli.parser)
        ParserDefaults.db_port(cli.parser)
        ParserDefaults.db_user(cli.parser)
        ParserDefaults.db_pass(cli.parser)
        ParserDefaults.db_name(cli.parser)
        ParserDefaults.qpid_polling_interval(cli.parser)
        ParserDefaults.qpid_max_update_per_second(cli.parser)
        ParserDefaults.qpid_cluster(cli.parser)
        ParserDefaults.qpid_uri(cli.parser)

        args = cli.parse_args()

        return cls(db_host=args.db_host, db_port=args.db_port, db_user=args.db_user,
                   db_pass=args.db_pass, db_name=args.db_name,
                   qpid_polling_interval=args.qpid_polling_interval,
                   max_update_per_second=args.qpid_max_update_per_second,
                   cluster=args.qpid_cluster, uri=args.qpid_uri)

    def __init__(self, db_host: str, db_port: str, db_user: str, db_pass: str,
                 db_name: str, qpid_polling_interval: int, max_update_per_second: int,
                 qpid_cluster: str, qpid_uri: str):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.qpid_polling_interval = qpid_polling_interval
        self.max_update_per_second = max_update_per_second
        self.qpid_cluster = qpid_cluster
        self.qpid_uri = qpid_uri

    @property
    def conn_str(self) -> str:
        conn_str = "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=self.db_user, pw=self.db_pass, host=self.db_host,
            port=self.db_port, db=self.db_name
        )
        return conn_str
