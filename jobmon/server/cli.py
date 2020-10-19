import configargparse
import logging
from typing import Optional


from jobmon.config import PARSER_KWARGS, ParserDefaults, CLI

logger = logging.getLogger(__name__)


class ServerCLI(CLI):

    def __init__(self) -> None:
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser
        )

        # now add specific sub parsers
        self._add_web_service_subparser()
        self._add_workflow_reaper_subparser()
        self._add_qpid_integration_subparser()

    def web_service(self, args: configargparse.Namespace) -> None:
        '''web service entrypoint logic'''
        from jobmon.server.web.api import create_app, WebConfig

        web_config = WebConfig(
            db_host=args.db_host, db_port=args.db_port, db_user=args.db_user,
            db_pass=args.db_pass, db_name=args.db_name)
        app = create_app(web_config)

        if args.command == 'start':
            raise ValueError('Web service cannot be started via command line.')
        elif args.command == 'test':
            app.run(host='0.0.0.0', port=args.web_service_port, debug=True,
                    use_reloader=False, use_evalex=False, threaded=False)
        else:
            raise ValueError('Invalid command choice. Options are (test, start), got '
                             f'({args.command})')

    def workflow_reaper(self, args: configargparse.Namespace) -> None:
        '''workflow reaper entrypoint logic'''
        from jobmon.server.workflow_reaper.api import (WorkflowReaperConfig,
                                                       start_workflow_reaper)
        reaper_config = WorkflowReaperConfig(
            poll_interval_minutes=args.poll_interval_minutes,
            loss_threshold=args.loss_threshold,
            host=args.web_service_fqdn,
            port=args.web_service_port,
            slack_api_url=args.slack_api_url,
            slack_token=args.slack_token,
            slack_channel_default=args.slack_channel_default
        )
        if args.command == 'start':
            start_workflow_reaper(reaper_config)
        else:
            raise ValueError('Invalid command choice. Options are (start), got '
                             f'({args.command})')

    def qpid_integration(self, args: configargparse.Namespace) -> None:
        from jobmon.server.qpid_integration.api import start_qpid_integration
        # TODO: need dependency injection into qpid integration
        if args.command == 'start':
            start_qpid_integration()
        else:
            raise ValueError('Invalid command choice. Options are (start), got '
                             f'({args.command})')

    def _add_web_service_subparser(self) -> None:
        web_service_parser = self._subparsers.add_parser('web_service', **PARSER_KWARGS)
        web_service_parser.set_defaults(func=self.web_service)
        web_service_parser.add_argument(
            'command',
            type=str,
            choices=['start', 'test'],
            help=('The web_server sub-command to run: (start, test). Start is not currently '
                  'supported. Test creates a test instance of the jobmon Flask app using the '
                  'Flask dev server and should not be used for production')
        )
        ParserDefaults.db_host(web_service_parser)
        ParserDefaults.db_port(web_service_parser)
        ParserDefaults.db_user(web_service_parser)
        ParserDefaults.db_pass(web_service_parser)
        ParserDefaults.db_name(web_service_parser)
        ParserDefaults.web_service_port(web_service_parser)

    def _add_workflow_reaper_subparser(self) -> None:
        reaper_parser = self._subparsers.add_parser('workflow_reaper', **PARSER_KWARGS)
        reaper_parser.set_defaults(func=self.workflow_reaper)
        reaper_parser.add_argument(
            'command',
            type=str,
            choices=['start'],
            help=('The workflow_reaper sub-command to run: (start). Start command runs '
                  'workflow_reaper.monitor_forever() method.')
        )
        ParserDefaults.reaper_poll_interval_minutes(reaper_parser)
        ParserDefaults.reaper_loss_threshold(reaper_parser)
        ParserDefaults.web_service_fqdn(reaper_parser)
        ParserDefaults.web_service_port(reaper_parser)
        ParserDefaults.slack_api_url(reaper_parser)
        ParserDefaults.slack_token(reaper_parser)
        ParserDefaults.slack_channel_default(reaper_parser)

    def _add_qpid_integration_subparser(self) -> None:
        qpid_parser = self._subparsers.add_parser('qpid_integration', **PARSER_KWARGS)
        qpid_parser.set_defaults(func=self.qpid_integration)
        qpid_parser.add_argument(
            'command',
            type=str,
            choices=['start'],
            help=('The qpid_integration sub-command to run: (start). Start command runs '
                  'qpid.maxpss_forever().')
        )


def main(argstr: Optional[str] = None) -> None:
    cli = ServerCLI()
    cli.main(argstr)
