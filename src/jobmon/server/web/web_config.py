"""Config specific to web services."""
from __future__ import annotations

from typing import Optional, Type

import sqlalchemy

from jobmon.config import CLI, ParserDefaults


class WebConfig:
    """web config class"""

    @classmethod
    def from_defaults(cls: Type[WebConfig]) -> WebConfig:
        """Defaults hierarchy is available from configargparse jobmon_cli."""
        cli = CLI()
        ParserDefaults.sql_dialect(cli.parser)
        if cli.parser.parse_known_args()[0].dialect == "mysql":
            ParserDefaults.db_host(cli.parser)
            ParserDefaults.db_port(cli.parser)
            ParserDefaults.db_user(cli.parser)
            ParserDefaults.db_pass(cli.parser)
            ParserDefaults.db_name(cli.parser)
        else:
            ParserDefaults.sqlite_file(cli.parser)
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

        if args.sql_dialect == "mysql":
            try:
                # default to mysqldb if installed. ~10x faster than pymysql
                import MySQLdb  # noqa F401
                driver = "mysqldb"
            except (ImportError, ModuleNotFoundError):
                # otherwise use pymysql since it is pip installable
                import pymysql  # noqa F401
                driver = "pymysql"

            conn_str = "mysql+{driver}://{user}:{pw}@{host}:{port}/{db}".format(
                driver=driver,
                user=args.db_user,
                pw=args.db_pass,
                host=args.db_host,
                port=args.db_port,
                db=args.db_name,
            )
            engine = sqlalchemy.create_engine(conn_str, pool_recycle=200, future=True)
        else:
            conn_str = f"sqlite://{args.sqlite_file}"
            engine = sqlalchemy.create_engine(
                conn_str,
                connect_args={'check_same_thread': False},
                poolclass=sqlalchemy.pool.StaticPool, future=True
            )

        return cls(
            engine=engine,
            use_logstash=args.use_logstash,
            logstash_host=args.logstash_host,
            logstash_port=args.logstash_port,
            logstash_protocol=args.logstash_protocol,
            use_apm=args.use_apm,
            apm_server_url=args.apm_server_url,
            apm_server_name=args.apm_server_name,
            apm_port=args.apm_port,
            log_level=args.log_level,
        )

    def __init__(
        self,
        engine: sqlalchemy.Engine,
        use_logstash: bool = False,
        logstash_host: str = "",
        logstash_port: Optional[int] = None,
        logstash_protocol: str = "",
        use_apm: bool = False,
        apm_server_url: str = "",
        apm_server_name: str = "",
        apm_port: Optional[int] = None,
        log_level: str = "INFO",
    ) -> None:
        """Initialize config for server."""
        self.engine = engine
        self.use_logstash = use_logstash
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.logstash_protocol = logstash_protocol
        self.use_apm = use_apm
        self.apm_server_url = apm_server_url
        self.apm_server_name = apm_server_name
        self.apm_port = apm_port
        self.log_level = log_level
