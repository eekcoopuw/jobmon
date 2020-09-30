from __future__ import annotations

from jobmon.config import CLI, ParserDefaults


class WebConfig(object):
    """
    This is intended to be a singleton. If using in any other way, proceed
    with CAUTION.
    """

    @classmethod
    def from_defaults(cls) -> WebConfig:
        # defaults hierarchy is available from configargparse cli
        cli = CLI()
        ParserDefaults.db_host(cli.parser)
        ParserDefaults.db_port(cli.parser)
        ParserDefaults.db_user(cli.parser)
        ParserDefaults.db_pass(cli.parser)
        ParserDefaults.db_name(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")
        return cls(db_host=args.db_host, db_port=args.db_port, db_user=args.db_user,
                   db_pass=args.db_pass, db_name=args.db_name)

    def __init__(self, db_host: str, db_port: str, db_user: str, db_pass: str,
                 db_name: str):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

    @property
    def conn_str(self) -> str:
        conn_str = "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=self.db_user, pw=self.db_pass, host=self.db_host,
            port=self.db_port, db=self.db_name
        )
        return conn_str
