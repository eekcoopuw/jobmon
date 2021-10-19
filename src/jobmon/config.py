"""Parse configuration options and set them to be used throughout the Jobmon Architecture."""
import os
import shlex
from typing import List, Optional

import configargparse

CONFIG_FILE_NAME = ".jobmon.ini"
INSTALLED_CONFIG_FILE = os.path.join(os.path.dirname(__file__), CONFIG_FILE_NAME)
HOMEDIR_CONFIG_FILE = os.path.join("~/", CONFIG_FILE_NAME)

PARSER_KWARGS = {
    "description": "Jobmon CLI",
    "default_config_files": [INSTALLED_CONFIG_FILE, HOMEDIR_CONFIG_FILE],
    "ignore_unknown_config_file_keys": True,
    "config_file_parser_class": configargparse.ConfigparserConfigFileParser,
    "args_for_setting_config_path": ["--config"],
}


def derive_jobmon_command_from_env() -> Optional[str]:
    """If a singularity path is provided, use it when running the worker node."""
    singularity_img_path = os.environ.get("IMGPATH", None)
    if singularity_img_path:
        return f"singularity run --app jobmon_command {singularity_img_path}"
    return None


class ParserDefaults:
    """Default config setup if not set by user."""

    @staticmethod
    def log_level(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Set the logging level."""
        parser.add_argument(
            "--log_level",
            type=str,
            help="what level of logging is desired",
            default="INFO",
            env_var="LOG_LEVEL",
        )
        return parser

    @staticmethod
    def logstash_host(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Set the logstash host to use if using it."""
        parser.add_argument(
            "--logstash_host",
            type=str,
            help="logstash host to use",
            default="logstash",
            env_var="LOGSTASH_HOST",
        )
        return parser

    @staticmethod
    def logstash_port(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Set the logstash port to use if using logstash."""
        parser.add_argument(
            "--logstash_port",
            type=int,
            help="logstash port to use",
            default=5000,
            env_var="LOGSTASH_PORT",
        )
        return parser

    @staticmethod
    def logstash_protocol(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Set the protocol to communicate with the logstash server if using logstash."""
        parser.add_argument(
            "--logstash_protocol",
            type=str,
            help="logstash protocol to use",
            default="UDP",
            choices=["TCP", "HTTP", "Beats", "UDP"],
            env_var="LOGSTASH_PROTOCOL",
        )
        return parser

    @staticmethod
    def use_logstash(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Boolean to use logstash or not."""
        parser.add_argument(
            "--use_logstash",
            help="whether to forward logs to logstash",
            default=False,
            env_var="USE_LOGSTASH",
            action="store_true",
        )
        return parser

    @staticmethod
    def use_apm(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        """Boolean to use logstash or not."""
        parser.add_argument(
            "--use_apm",
            help="whether to use APM",
            default=False,
            env_var="USE_APM",
            action="store_true",
        )
        return parser

    @staticmethod
    def apm_server_name(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Server name of APM."""
        parser.add_argument(
            "--apm_server_name",
            type=str,
            help="Server name of APM",
            default="jobmon-apm",
            env_var="APM_SERVER_NAME",
        )
        return parser

    @staticmethod
    def apm_server_url(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Server URL of APM."""
        parser.add_argument(
            "--apm_server_url",
            type=str,
            help="Server URL of APM",
            default="jobmon-apm",
            env_var="APM_SERVER_URL",
        )
        return parser

    @staticmethod
    def apm_port(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Set the APM port to use if using APM."""
        parser.add_argument(
            "--apm_port",
            type=int,
            help="APM port to use",
            default=8200,
            env_var="APM_PORT",
        )
        return parser

    @staticmethod
    def db_host(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        """Host running the Jobmon DB."""
        parser.add_argument(
            "--db_host",
            type=str,
            help="database host to use",
            required=True,
            env_var="DB_HOST",
        )
        return parser

    @staticmethod
    def db_port(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        """Port to connect to the Jobmon DB on."""
        parser.add_argument(
            "--db_port",
            type=str,
            help="database port to use",
            required=True,
            env_var="DB_PORT",
        )
        return parser

    @staticmethod
    def db_user(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        """DB username to use to connect to the Jobmon DB."""
        parser.add_argument(
            "--db_user",
            type=str,
            help="database user to use",
            required=True,
            env_var="DB_USER",
        )
        return parser

    @staticmethod
    def db_pass(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        """Password to use to connect to the Jobmon DB."""
        parser.add_argument(
            "--db_pass",
            type=str,
            help="database password to use",
            required=True,
            env_var="DB_PASS",
        )
        return parser

    @staticmethod
    def db_name(parser: configargparse.ArgumentParser) -> configargparse.ArgumentParser:
        """Name of the Jobmon DB you want to connect to."""
        parser.add_argument(
            "--db_name",
            type=str,
            help="default database to use",
            default="docker",
            env_var="DB_NAME",
        )
        return parser

    @staticmethod
    def web_service_fqdn(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Fully qualified domain name of the Jobmon web service."""
        parser.add_argument(
            "--web_service_fqdn",
            type=str,
            help="fully qualified domain name of web service",
            required=True,
            env_var="WEB_SERVICE_FQDN",
        )
        return parser

    @staticmethod
    def web_service_port(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Port that Jobmon flask web service is available on."""
        parser.add_argument(
            "--web_service_port",
            type=str,
            help="port that web service is listening on",
            required=True,
            env_var="WEB_SERVICE_PORT",
        )
        return parser

    @staticmethod
    def reaper_poll_interval_minutes(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Duration in minutes to sleep between reaper loops."""
        parser.add_argument(
            "--reaper_poll_interval_minutes",
            type=int,
            help="Duration in minutes to sleep between reaper loops",
            default=10,
            env_var="REAPER_POLL_INTERVAL_MINUTES",
        )
        return parser

    @staticmethod
    def slack_api_url(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """URL to post notifications to if using Slack."""
        parser.add_argument(
            "--slack_api_url",
            type=str,
            help="URL to post notifications",
            default="https://slack.com/apis/chat.postMessage",
            env_var="SLACK_API_URL",
        )
        return parser

    @staticmethod
    def slack_token(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Authentication token for posting updates to slack."""
        parser.add_argument(
            "--slack_token",
            type=str,
            help="Authentication token for posting updates to slack",
            default="",
            env_var="SLACK_TOKEN",
        )
        return parser

    @staticmethod
    def slack_channel_default(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Default slack channel to post updates to."""
        parser.add_argument(
            "--slack_channel_default",
            type=str,
            help="Default channel to post updates to",
            default="jobmon-alerts",
            env_var="SLACK_CHANNEL_DEFAULT",
        )
        return parser

    @staticmethod
    def qpid_polling_interval(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Interval between qpid polling cycles if connecting qpid service."""
        parser.add_argument(
            "--qpid_polling_interval",
            type=int,
            help="Interval between qpid polling cycles",
            default=600,
            env_var="QPID_POLLING_INTERVAL",
        )
        return parser

    @staticmethod
    def qpid_max_update_per_second(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Number of maxpss updates per second."""
        parser.add_argument(
            "--qpid_max_update_per_second",
            type=int,
            help="Amount of maxpss updates per second",
            default=10,
            env_var="QPID_MAX_UPDATE_PER_SECOND",
        )
        return parser

    @staticmethod
    def qpid_cluster(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Cluster to pull maxpss data from. Default is fair."""
        parser.add_argument(
            "--qpid_cluster",
            type=str,
            help="which cluster to pull maxpss for",
            default="fair",
            env_var="QPID_CLUSTER",
        )
        return parser

    @staticmethod
    def qpid_uri(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """URI for qpid service if using qpid service."""
        parser.add_argument(
            "--qpid_uri",
            type=str,
            help="uri for qpid service",
            required=True,
            env_var="QPID_URI",
        )
        return parser

    @staticmethod
    def worker_node_entry_point(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Entry point to execute on worker node to run a task instance."""
        parser.add_argument(
            "--worker_node_entry_point",
            type=str,
            help="Entry point to execute on worker node to run a task instance",
            default=derive_jobmon_command_from_env(),
            env_var="WORKER_NODE_ENTRY_POINT",
        )
        return parser

    @staticmethod
    def workflow_run_heartbeat_interval(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Interval at which workflow run logs a heartbeat."""
        parser.add_argument(
            "--workflow_run_heartbeat_interval",
            type=int,
            help="",
            default=30,
            env_var="WORKFLOW_RUN_HEARTBEAT_INTERVAL",
        )
        return parser

    @staticmethod
    def task_instance_heartbeat_interval(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Entry point to execute on worker node to run a task instance."""
        parser.add_argument(
            "--task_instance_heartbeat_interval",
            type=int,
            help="Entry point to execute on worker node to run a task instance",
            default=90,
            env_var="TASK_INSTANCE_HEARTBEAT_INTERVAL",
        )
        return parser

    @staticmethod
    def heartbeat_report_by_buffer(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """Multiplier for heartbeat interval that can be missed before job is lost."""
        parser.add_argument(
            "--heartbeat_report_by_buffer",
            type=float,
            help="Multiplier for heartbeat interval that can be missed before job is lost",
            default=3.1,
            env_var="HEARTBEAT_REPORT_BY_BUFFER",
        )
        return parser

    @staticmethod
    def distributor_n_queued(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """How many jobs to distribute in one distributor loop."""
        parser.add_argument(
            "--distributor_n_queued",
            type=int,
            help="How many jobs to distribute in one distributor loop",
            default=100,
            env_var="DISTRIBUTOR_N_QUEUED",
        )
        return parser

    @staticmethod
    def distributor_poll_interval(
        parser: configargparse.ArgumentParser,
    ) -> configargparse.ArgumentParser:
        """How long to sleep between distributor loops."""
        parser.add_argument(
            "--distributor_poll_interval",
            type=int,
            help="How long to sleep between distributor loops",
            default=10,
            env_var="DISTRIBUTOR_POLL_INTERVAL",
        )
        return parser


class CLI:
    """Base CLI."""

    def __init__(self) -> None:
        """Initialize the CLI."""
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)

    def main(self, argstr: Optional[str] = None) -> None:
        """Parse args."""
        args = self.parse_args(argstr)
        args.func(args)

    def parse_args(self, argstr: Optional[str] = None) -> configargparse.Namespace:
        """Construct a parser, parse either sys.argv (default) or the provided argstr.

        Returns a Namespace. The Namespace should have a 'func' attribute which can be used to
        dispatch to the appropriate downstream function.
        """
        arglist: Optional[List[str]] = None
        if argstr is not None:
            arglist = shlex.split(argstr)

        args = self.parser.parse_args(arglist)

        return args
