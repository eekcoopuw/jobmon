"""Configuration specific to worker node."""
from typing import Any

from jobmon.config import CLI, ParserDefaults


class WorkerNodeConfig:
    """Configuration specific to worker node."""

    @classmethod
    def from_defaults(cls: Any) -> Any:
        """If no special config set up, use defaults to set config."""
        cli = CLI()

        ParserDefaults.task_instance_heartbeat_interval(cli.parser)
        ParserDefaults.heartbeat_report_by_buffer(cli.parser)
        ParserDefaults.web_service_fqdn(cli.parser)
        ParserDefaults.web_service_port(cli.parser)

        # passing an empty string forces this method to ignore sys.argv
        args = cli.parse_args("")

        return cls(
            task_instance_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port,
        )

    def __init__(self, task_instance_heartbeat_interval: int,
                 heartbeat_report_by_buffer: float,
                 web_service_fqdn: str, web_service_port: str) -> None:
        """Initialization of the worker node config."""
        self.task_instance_heartbeat_interval = task_instance_heartbeat_interval
        self.heartbeat_report_by_buffer = heartbeat_report_by_buffer
        self.web_service_fqdn = web_service_fqdn
        self.web_service_port = web_service_port

    @property
    def url(self) -> str:
        """URL to connect to the jobmon flask web services."""
        return f"http://{self.web_service_fqdn}:{self.web_service_port}"
