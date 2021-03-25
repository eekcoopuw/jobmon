"""Config specific to web services."""
from __future__ import annotations

try:
    # default to mysqldb if installed. ~10x faster than pymysql
    import MySQLdb  # noqa F401
    driver = "mysqldb"
except (ImportError, ModuleNotFoundError):
    # otherwise use pymysql since it is pip installable
    import pymysql  # noqa F401
    driver = "pymysql"


from jobmon.config import CLI, ParserDefaults


class WebConfig(object):
    """This is intended to be a singleton. If using in another way, proceed with CAUTION."""

    @classmethod
    def from_defaults(cls) -> WebConfig:
        """Defaults hierarchy is available from configargparse jobmon_cli."""
        cli = CLI()
        ParserDefaults.db_host(cli.parser)
        ParserDefaults.db_port(cli.parser)
        ParserDefaults.db_user(cli.parser)
        ParserDefaults.db_pass(cli.parser)
        ParserDefaults.db_name(cli.parser)
        ParserDefaults.use_logstash(cli.parser)
        ParserDefaults.logstash_host(cli.parser)
        ParserDefaults.logstash_port(cli.parser)
        ParserDefaults.logstash_protocol(cli.parser)
        ParserDefaults.use_apm(cli.parser)
        ParserDefaults.apm_server_url(cli.parser)
        ParserDefaults.apm_server_name(cli.parser)
        ParserDefaults.apm_port(cli.parser)
        ParserDefaults.log_level(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")
        return cls(db_host=args.db_host, db_port=args.db_port, db_user=args.db_user,
                   db_pass=args.db_pass, db_name=args.db_name, use_logstash=args.use_logstash,
                   logstash_host=args.logstash_host, logstash_port=args.logstash_port,
                   logstash_protocol=args.logstash_protocol, use_apm=args.use_apm,
                   apm_server_url=args.apm_server_url, apm_server_name=args.apm_server_name,
                   apm_port=args.apm_port, log_level=args.log_level)

    def __init__(self, db_host: str, db_port: str, db_user: str, db_pass: str,
                 db_name: str, use_logstash: bool = False, logstash_host: str = "",
                 logstash_port: int = None, logstash_protocol: str = "", use_apm: bool = False,
                 apm_server_url: str = "", apm_server_name: str = "", apm_port: int = None,
                 log_level: str = "INFO"):
        """Initialize config for server."""
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.use_logstash = use_logstash
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.logstash_protocol = logstash_protocol
        self.use_apm = use_apm
        self.apm_server_url = apm_server_url
        self.apm_server_name = apm_server_name
        self.apm_port = apm_port
        self.log_level = log_level

    @property
    def conn_str(self) -> str:
        """Database connection string."""
        conn_str = "mysql+{driver}://{user}:{pw}@{host}:{port}/{db}".format(
            driver=driver, user=self.db_user, pw=self.db_pass, host=self.db_host,
            port=self.db_port, db=self.db_name
        )
        return conn_str
