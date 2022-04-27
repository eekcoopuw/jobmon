"""Command line interface for Execution."""
import logging
from typing import Optional

import configargparse

from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults


class DistributorCLI(CLI):
    """Command line interface for Distributor."""

    def __init__(self) -> None:
        """Initialization of distributor CLI."""
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=configargparse.ArgumentParser
        )

        self._add_distributor_parser()

    def distributor(self, args: configargparse.Namespace) -> None:
        """Configuration for the jobmon worker node."""
        from jobmon.client.client_logging import ClientLogging
        from jobmon.client.distributor.api import (
            get_distributor_service,
            DistributorConfig,
        )

        #ClientLogging(log_level=logging.DEBUG).attach("jobmon.client.distributor")
        #distributor_logger = logging.getLogger("jobmon.client.distributor")
        #distributor_logger.setLevel(logging.INFO)

        distributor_config = DistributorConfig(
            worker_node_entry_point=args.worker_node_entry_point,
            task_instance_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            distributor_poll_interval=args.distributor_poll_interval,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port,
        )

        # TODO: how do we pass in executor args
        distributor_service = get_distributor_service(
            args.cluster_name, distributor_config
        )
        distributor_service.set_workflow_run(args.workflow_run_id)
        distributor_service.run()

    def _add_distributor_parser(self) -> None:
        distributor_parser = self._subparsers.add_parser("start", **PARSER_KWARGS)
        distributor_parser.set_defaults(func=self.distributor)
        distributor_parser.add_argument(
            "--cluster_name",
            type=str,
            help="cluster_name to distribute jobs onto.",
            required=True,
        )
        distributor_parser.add_argument(
            "--workflow_run_id",
            type=int,
            help="workflow_run_id to distribute jobs for.",
            required=True,
        )
        ParserDefaults.worker_node_entry_point(distributor_parser)
        ParserDefaults.task_instance_heartbeat_interval(distributor_parser)
        ParserDefaults.heartbeat_report_by_buffer(distributor_parser)
        ParserDefaults.distributor_poll_interval(distributor_parser)
        ParserDefaults.web_service_fqdn(distributor_parser)
        ParserDefaults.web_service_port(distributor_parser)


def main(argstr: Optional[str] = None) -> None:
    """Entrypoint to create Executor CLI."""
    cli = DistributorCLI()
    cli.main(argstr)


if __name__ == "__main__":
    main()
