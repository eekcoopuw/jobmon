"""Command line interface for Execution."""
from typing import Optional

import configargparse

from jobmon.cluster_type.api import import_cluster
from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults


class DistributorCLI(CLI):
    """Command line interface for Distributor."""

    def __init__(self) -> None:
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser
        )

        self._add_distributor_parser()

    def distributor(self, args: configargparse.Namespace) -> None:
        """Configuration for the jobmon worker node."""
        from jobmon.client.distributor.api import get_task_instance_distributor, DistributorConfig

        distributor_config = DistributorConfig(
            jobmon_command=args.worker_node_entry_point,
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            task_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            n_queued=args.distributor_n_queued,
            distributor_poll_interval=args.distributor_poll_interval,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port
        )

        # Minimum args: args.workflow_id, args.workflow_run_id, args.cluster_type_name
        module = import_cluster(args.cluster_type_name)
        ClusterDistributor = module.get_cluster_distributor_class()
        distributor = ClusterDistributor()

        # TODO: how do we pass in executor args
        if args.command == 'start':
            task_instance_distributor = \
                get_task_instance_distributor(args.workflow_id,
                                              args.workflow_run_id,
                                              distributor,
                                              distributor_config)
            task_instance_distributor.run_distributor()
        else:
            raise ValueError(f"Command {args.command} not supported.")

    def _add_distributor_parser(self) -> None:
        distributor_parser = self._subparsers.add_parser('distributor', **PARSER_KWARGS)
        distributor_parser.set_defaults(func=self.distributor)
        distributor_parser.add_argument(
            'command',
            type=str,
            choices=['start'],
            help=('The distributor sub-command to run: (start)'),
        )
        distributor_parser.add_argument(
            '--workflow_id',
            type=int,
            help='workflow_id to distribute jobs for.',
            required=True
        )
        distributor_parser.add_argument(
            '--workflow_run_id',
            type=int,
            help='workflow_run_id to distribute jobs for.',
            required=True
        )
        ParserDefaults.worker_node_entry_point(distributor_parser)
        ParserDefaults.workflow_run_heartbeat_interval(distributor_parser)
        ParserDefaults.task_instance_heartbeat_interval(distributor_parser)
        ParserDefaults.heartbeat_report_by_buffer(distributor_parser)
        ParserDefaults.distributor_n_queued(distributor_parser)
        ParserDefaults.distributor_poll_interval(distributor_parser)
        ParserDefaults.web_service_fqdn(distributor_parser)
        ParserDefaults.web_service_port(distributor_parser)


def main(argstr: Optional[str] = None) -> None:
    """Entrypoint to create Executor CLI."""
    cli = DistributorCLI()
    cli.main(argstr)
