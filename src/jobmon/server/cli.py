"""Set up server specific CLI config."""
import logging
from typing import Optional

import configargparse

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
        self._add_web_service_subparser()
        self._add_workflow_reaper_subparser()
        self._add_qpid_integration_subparser()

    def web_service(self, args: configargparse.Namespace) -> None:
        """Web service entrypoint logic."""
        from jobmon.server.web.api import create_app, WebConfig

        web_config = WebConfig(
            db_host=args.db_host,
            db_port=args.db_port,
            db_user=args.db_user,
            db_pass=args.db_pass,
            db_name=args.db_name,
            logstash_host=args.logstash_host,
            logstash_port=args.logstash_port,
            logstash_protocol=args.logstash_protocol,
            use_logstash=args.use_logstash,
            use_apm=args.use_apm,
            apm_server_url=args.apm_server_url,
            apm_server_name=args.apm_server_name,
            apm_port=args.apm_port,
        )

        app = create_app(web_config)

        if args.command == "start":
            raise ValueError("Web service cannot be started via command line.")
        elif args.command == "test":
            app.run(host="0.0.0.0", port=args.web_service_port)
        elif args.command == "start_uwsgi":
            import subprocess

            subprocess.run("/entrypoint.sh")
            subprocess.run("/start.sh")
        else:
            raise ValueError(
                "Invalid command choice. Options are (test, start and "
                f"start_uwsgi), got ({args.command})"
            )

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

    def qpid_integration(self, args: configargparse.Namespace) -> None:
        """QPID integration service entrypoint logic."""
        from jobmon.server.squid_integration.api import start_squid_integration

        # TODO: need dependency injection into qpid integration
        if args.command == "start":
            start_squid_integration()
        else:
            raise ValueError(
                "Invalid command choice. Options are (start), got " f"({args.command})"
            )

    def _add_web_service_subparser(self) -> None:
        web_service_parser = self._subparsers.add_parser("web_service", **PARSER_KWARGS)
        web_service_parser.set_defaults(func=self.web_service)
        web_service_parser.add_argument(
            "command",
            type=str,
            choices=["start", "test", "start_uwsgi"],
            help=(
                "The web_server sub-command to run: (start, test, start_uwsgi). Start is not"
                " currently supported. Test creates a test instance of the jobmon Flask app "
                "using the Flask dev server and should not be used for production"
            ),
        )
        ParserDefaults.db_host(web_service_parser)
        ParserDefaults.db_port(web_service_parser)
        ParserDefaults.db_user(web_service_parser)
        ParserDefaults.db_pass(web_service_parser)
        ParserDefaults.db_name(web_service_parser)
        ParserDefaults.web_service_port(web_service_parser)
        ParserDefaults.logstash_host(web_service_parser)
        ParserDefaults.logstash_port(web_service_parser)
        ParserDefaults.logstash_protocol(web_service_parser)
        ParserDefaults.use_logstash(web_service_parser)
        ParserDefaults.use_apm(web_service_parser)
        ParserDefaults.apm_server_url(web_service_parser)
        ParserDefaults.apm_server_name(web_service_parser)
        ParserDefaults.apm_port(web_service_parser)

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

    def _add_qpid_integration_subparser(self) -> None:
        qpid_parser = self._subparsers.add_parser("squid_integration", **PARSER_KWARGS)
        qpid_parser.set_defaults(func=self.qpid_integration)
        qpid_parser.add_argument(
            "command",
            type=str,
            choices=["start"],
            help=(
                "The squid_integration sub-command to run: (start). Start command runs "
                "squid.maxrss_forever()."
            ),
        )


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ServerCLI()
    cli.main(argstr)
