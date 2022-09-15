"""Set up server specific CLI config."""
import logging
from typing import Optional

import argparse

from jobmon.cli import CLI

logger = logging.getLogger(__name__)


class ServerCLI(CLI):
    """CLI for Server only."""

    def __init__(self) -> None:
        """Initialize ServerCLI with subcommands."""
        self.parser = argparse.ArgumentParser("jobmon server")
        self._subparsers = self.parser.add_subparsers(dest="sub_command")

        # now add specific sub parsers
        self._add_web_service_subparser()
        # self._add_workflow_reaper_subparser()
        # self._add_integrator_subparser()
        # self._add_init_db_subparser()
        # self._add_terminate_db_subparser()

    def web_service(self, args: argparse.Namespace) -> None:
        """Web service entrypoint logic."""
        from jobmon.server.web.api import AppFactory

        app_factory = AppFactory(sqlalchemy_database_uri=args.sqlalchemy_database_uri)
        app = app_factory.get_app()
        with app.app_context():
            app.run(host="0.0.0.0", port=args.port)

    def workflow_reaper(self, args: argparse.Namespace) -> None:
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

    def integration(self, args: argparse.Namespace) -> None:
        """Integration service entrypoint logic."""
        # TODO: need dependency injection into squid integration
        from jobmon.server.usage_integration.api import start_usage_integration

        if args.command == "start":
            start_usage_integration()
        else:
            raise ValueError(
                "Invalid command choice. Options are (start), got " f"({args.command})"
            )

    def init_db(self, args: argparse.Namespace) -> None:
        """Entrypoint to initialize new Jobmon database."""
        import sqlalchemy
        from jobmon.server.web.models import init_db, terminate_db

        engine = sqlalchemy.create_engine(args.sqlalchemy_database_uri)
        try:
            init_db(engine)
        except Exception:
            terminate_db(engine)
            raise

    def terminate_db(self, args: argparse.Namespace) -> None:
        """Entrypoint to terminate a Jobmon database."""
        import sqlalchemy
        from jobmon.server.web.models import terminate_db

        terminate_db(sqlalchemy.create_engine(args.sqlalchemy_database_uri))

    def _add_web_service_subparser(self) -> None:
        web_service_parser = self._subparsers.add_parser("web_service")
        web_service_parser.set_defaults(func=self.web_service)
        web_service_parser.add_argument(
            "--port",
            type=int,
            help="port that web service is listening on",
            required=True,
        )
        web_service_parser.add_argument(
            "--sqlalchemy_database_uri",
            type=str,
            help="The connection string for sqlalchemy to use when running the server.",
            required=False,
            default=""
        )

    def _add_workflow_reaper_subparser(self) -> None:
        reaper_parser = self._subparsers.add_parser("workflow_reaper")
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
        integrator_parser = self._subparsers.add_parser("integration")
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

    def _add_init_db_subparser(self) -> None:
        init_db_parser = self._subparsers.add_parser("init_db")
        init_db_parser.set_defaults(func=self.init_db)
        init_db_parser.add_argument(
            "--sqlalchemy_database_uri",
            type=str,
            help="The connection string for sqlalchemy to use when running the server.",
            required=False,
            default=""
        )

    def _add_terminate_db_subparser(self) -> None:
        terminate_db_parser = self._subparsers.add_parser("terminate_db")
        terminate_db_parser.set_defaults(func=self.terminate_db)
        terminate_db_parser.add_argument(
            "--sqlalchemy_database_uri",
            type=str,
            help="The connection string for sqlalchemy to use when running the server.",
            required=False,
            default=""
        )


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ServerCLI()
    cli.main(argstr)
