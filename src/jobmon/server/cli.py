"""Set up server specific CLI config."""
import logging
from typing import Optional

import configargparse
import sqlalchemy

from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults

logger = logging.getLogger(__name__)


class ServerCLI(CLI):
    """CLI for Server only."""

    def __init__(self) -> None:
        """Initialize ServerCLI with subcommands."""
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=configargparse.ArgumentParser
        )

        # now add specific sub parsers
        self._add_web_service_sqlite_subparser()
        self._add_web_service_mysql_subparser()
        self._add_workflow_reaper_subparser()
        self._add_integrator_subparser()

    def web_service_sqlite(self, args: configargparse.Namespace) -> None:
        """Web service entrypoint logic."""

        conn_str = f"sqlite://{args.sqlite_file}"
        engine = sqlalchemy.create_engine(
            conn_str, future=True
        )
        from jobmon.server.web.models import init_db
        init_db(engine)

        from jobmon.server.web.api import WebConfig
        from jobmon.server.web.app_factory import AppFactory
        web_config = WebConfig(
            engine=engine,
            logstash_host=args.logstash_host,
            logstash_port=args.logstash_port,
            logstash_protocol=args.logstash_protocol,
            use_logstash=args.use_logstash,
            use_apm=args.use_apm,
            apm_server_url=args.apm_server_url,
            apm_server_name=args.apm_server_name,
            apm_port=args.apm_port,
        )
        app_factory = AppFactory(web_config)
        app = app_factory.create_app_context()
        app.run(host="0.0.0.0", port=args.web_service_port)

    def web_service_mysql(self, args: configargparse.Namespace) -> None:
        """Web service entrypoint logic."""
        from jobmon.server.web.api import WebConfig
        from jobmon.server.web.app_factory import AppFactory

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

        web_config = WebConfig(
            engine=engine,
            logstash_host=args.logstash_host,
            logstash_port=args.logstash_port,
            logstash_protocol=args.logstash_protocol,
            use_logstash=args.use_logstash,
            use_apm=args.use_apm,
            apm_server_url=args.apm_server_url,
            apm_server_name=args.apm_server_name,
            apm_port=args.apm_port,
        )
        app_factory = AppFactory(web_config)
        app = app_factory.create_app_context()
        app.run(host="0.0.0.0", port=args.web_service_port)

    def workflow_reaper(self, args: configargparse.Namespace) -> None:
        """Workflow reaper entrypoint logic."""
        from jobmon.server.workflow_reaper.api import (
            WorkflowReaperConfig,
            start_workflow_reaper,
        )

        reaper_config = WorkflowReaperConfig(
            poll_interval_minutes=args.reaper_poll_interval_minutes,
            host=args.web_service_fqdn,
            port=args.web_service_port,
            slack_api_url=args.slack_api_url,
            slack_token=args.slack_token,
            slack_channel_default=args.slack_channel_default,
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
        )
        if args.command == "start":
            start_workflow_reaper(reaper_config)
        else:
            raise ValueError(
                "Invalid command choice. Options are (start), got " f"({args.command})"
            )

    def integration(self, args: configargparse.Namespace) -> None:
        """Integration service entrypoint logic."""
        # TODO: need dependency injection into squid integration
        from jobmon.server.usage_integration.api import start_usage_integration

        if args.command == "start":
            start_usage_integration()
        else:
            raise ValueError(
                "Invalid command choice. Options are (start), got " f"({args.command})"
            )

    def _add_web_service_sqlite_subparser(self) -> None:
        web_service_sqlite_parser = self._subparsers.add_parser("web_service_sqlite",
                                                                **PARSER_KWARGS)
        web_service_sqlite_parser.set_defaults(func=self.web_service_sqlite)
        ParserDefaults.sqlite_file(web_service_sqlite_parser)
        ParserDefaults.web_service_port(web_service_sqlite_parser)
        ParserDefaults.logstash_host(web_service_sqlite_parser)
        ParserDefaults.logstash_port(web_service_sqlite_parser)
        ParserDefaults.logstash_protocol(web_service_sqlite_parser)
        ParserDefaults.use_logstash(web_service_sqlite_parser)
        ParserDefaults.use_apm(web_service_sqlite_parser)
        ParserDefaults.apm_server_url(web_service_sqlite_parser)
        ParserDefaults.apm_server_name(web_service_sqlite_parser)
        ParserDefaults.apm_port(web_service_sqlite_parser)

    def _add_web_service_mysql_subparser(self) -> None:
        web_service_mysql_parser = self._subparsers.add_parser("web_service_mysql",
                                                               **PARSER_KWARGS)
        web_service_mysql_parser.set_defaults(func=self.web_service_mysql)
        ParserDefaults.db_host(web_service_mysql_parser)
        ParserDefaults.db_port(web_service_mysql_parser)
        ParserDefaults.db_user(web_service_mysql_parser)
        ParserDefaults.db_pass(web_service_mysql_parser)
        ParserDefaults.db_name(web_service_mysql_parser)
        ParserDefaults.sqlite_file(web_service_mysql_parser)
        ParserDefaults.web_service_port(web_service_mysql_parser)
        ParserDefaults.logstash_host(web_service_mysql_parser)
        ParserDefaults.logstash_port(web_service_mysql_parser)
        ParserDefaults.logstash_protocol(web_service_mysql_parser)
        ParserDefaults.use_logstash(web_service_mysql_parser)
        ParserDefaults.use_apm(web_service_mysql_parser)
        ParserDefaults.apm_server_url(web_service_mysql_parser)
        ParserDefaults.apm_server_name(web_service_mysql_parser)
        ParserDefaults.apm_port(web_service_mysql_parser)

    def _add_workflow_reaper_subparser(self) -> None:
        reaper_parser = self._subparsers.add_parser("workflow_reaper", **PARSER_KWARGS)
        reaper_parser.set_defaults(func=self.workflow_reaper)
        reaper_parser.add_argument(
            "command",
            type=str,
            choices=["start"],
            help=(
                "The workflow_reaper sub-command to run: (start). Start command runs "
                "workflow_reaper.monitor_forever() method."
            ),
        )
        ParserDefaults.reaper_poll_interval_minutes(reaper_parser)
        ParserDefaults.web_service_fqdn(reaper_parser)
        ParserDefaults.web_service_port(reaper_parser)
        ParserDefaults.slack_api_url(reaper_parser)
        ParserDefaults.slack_token(reaper_parser)
        ParserDefaults.slack_channel_default(reaper_parser)
        ParserDefaults.workflow_run_heartbeat_interval(reaper_parser)
        ParserDefaults.heartbeat_report_by_buffer(reaper_parser)

    def _add_integrator_subparser(self) -> None:
        integrator_parser = self._subparsers.add_parser("integration", **PARSER_KWARGS)
        integrator_parser.set_defaults(func=self.integration)
        integrator_parser.add_argument(
            "command",
            type=str,
            choices=["start"],
            help=(
                "The integrator sub-command to run: (start). Start command runs "
                "usage_integration.maxrss_forever()."
            ),
        )


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ServerCLI()
    cli.main(argstr)
