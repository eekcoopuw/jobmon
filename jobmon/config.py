import configargparse
import os
import shlex
from typing import Optional, List


CONFIG_FILE_NAME = '.jobmon.ini'
INSTALLED_CONFIG_FILE = os.path.join(os.path.dirname(__file__), CONFIG_FILE_NAME)
HOMEDIR_CONFIG_FILE = os.path.join('~/', CONFIG_FILE_NAME)


PARSER_KWARGS = {
    'description': 'Jobmon Server',
    'default_config_files': [INSTALLED_CONFIG_FILE, HOMEDIR_CONFIG_FILE],
    'auto_env_var_prefix': "JM_",
    'ignore_unknown_config_file_keys': True,
    'config_file_parser_class': configargparse.ConfigparserConfigFileParser,
    'args_for_setting_config_path': ["--config"]
}


class ParserDefaults:

    @staticmethod
    def db_host(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--db_host',
            type=str,
            help='database host to use',
            required=True
        )
        return parser

    @staticmethod
    def db_port(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--db_port',
            type=str,
            help='database port to use',
            required=True
        )
        return parser

    @staticmethod
    def db_user(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--db_user',
            type=str,
            help='database user to use',
            required=True
        )
        return parser

    @staticmethod
    def db_pass(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--db_pass',
            type=str,
            help='database password to use',
            required=True
        )
        return parser

    @staticmethod
    def db_name(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--db_name',
            type=str,
            help='default database to use',
            default='docker'
        )
        return parser

    @staticmethod
    def web_service_fqdn(parser: configargparse.ArgumentParser
                         ) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--web_service_fqdn',
            type=str,
            help='fully qualified domain name of web service',
            required=True
        )
        return parser

    @staticmethod
    def web_service_port(parser: configargparse.ArgumentParser,
                         ) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--web_service_port',
            type=str,
            help='port that web service is listening on',
            required=True
        )
        return parser

    @staticmethod
    def poll_interval_minutes(parser: configargparse.ArgumentParser,
                              ) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--poll_interval_minutes',
            type=int,
            help='Duration in minutes to sleep between reaper loops',
            default=10
        )
        return parser

    @staticmethod
    def loss_threshold(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--loss_threshold',
            type=int,
            help='Time to wait before reaping a workflow',
            default=5
        )
        return parser

    @staticmethod
    def slack_api_url(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--slack_api_url',
            type=str,
            help='URL to post notifications',
            default=''
        )
        return parser

    @staticmethod
    def slack_token(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--slack_token',
            type=str,
            help='Authentication token for posting updates to slack',
            default=''
        )
        return parser

    @staticmethod
    def slack_channel_default(parser: configargparse.ArgumentParser
                              ) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--slack_channel_default',
            type=str,
            help='Default channel to post updates to',
            default='jobmon-alerts'
        )
        return parser

    @staticmethod
    def qpid_polling_interval(parser: configargparse.ArgumentParser
                              ) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--qpid_polling_interval',
            type=int,
            help='Interval between qpid polling cycles',
            default=600
        )
        return parser

    @staticmethod
    def qpid_max_update_per_second(parser: configargparse.ArgumentParser
                                   ) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--qpid_max_update_per_second',
            type=int,
            help='Amount of maxpss updates per second',
            default=10
        )
        return parser

    @staticmethod
    def qpid_cluster(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--qpid_cluster',
            type=str,
            help='which cluster to pull maxpss for',
            default='fair'
        )
        return parser

    @staticmethod
    def qpid_uri(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        parser.add_argument(
            '--qpid_uri',
            type=str,
            help='uri for qpid service',
            required=True
        )
        return parser


class CLI:

    def __init__(self) -> None:
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)

    def main(self) -> None:
        args = self.parse_args()
        args.func(args)

    def parse_args(self, argstr: str = None) -> configargparse.Namespace:
        '''Construct a parser, parse either sys.argv (default) or the provided
        argstr, returns a Namespace. The Namespace should have a 'func'
        attribute which can be used to dispatch to the appropriate downstream
        function
        '''
        arglist: Optional[List[str]] = None
        if argstr is not None:
            arglist = shlex.split(argstr)

        args = self.parser.parse_args(arglist)

        return args
