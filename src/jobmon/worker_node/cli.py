"""Command line interface for Execution."""
from typing import Optional

import configargparse

from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults
from jobmon.exceptions import ReturnCodes
from jobmon.worker_node.execution_wrapper import unwrap


class WorkerNodeCLI(CLI):
    """Command line interface for WorkderNode."""

    def __init__(self) -> None:
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser
        )

        self._add_worker_node_parser()

    def run_task(self, args: configargparse.Namespace) -> ReturnCodes:
        """Configuration for the jobmon scheduler."""
        from jobmon.worker_node.worker_node_config import WorkerNodeConfig

        worker_node_config = WorkerNodeConfig(
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            task_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port
        )

        # run the job and return the exit code
        return unwrap(task_instance_id=worker_node_config.task_instance_id,
                      expected_jobmon_version=worker_node_config.expected_jobmon_version,
                      cluster_type_name=worker_node_config.cluster_type_name,
                      heartbeat_interval=worker_node_config.task_instance_heartbeat_interval,
                      report_by_buffer=worker_node_config.heartbeat_report_by_buffer)


    def _add_worker_node_parser(self) -> None:
        worker_node_parser = self._subparsers.add_parser('worker_node', **PARSER_KWARGS)
        worker_node_parser.set_defaults(func=self.scheduler)
        worker_node_parser.add_argument(
            '--task_instance_id',
            type=int,
            help='task_instance_id of the work node.',
            required=True
        )
        worker_node_parser.add_argument(
            '--cluster_type_name',
            type=str,
            help='cluster_type_name of the work node.',
            required=True
        )
        worker_node_parser.add_argument(
            '--expected_jobmon_version',
            type=str,
            help='expected_jobmon_version of the work node.',
            required=True
        )
        ParserDefaults.workflow_run_heartbeat_interval(worker_node_parser)
        ParserDefaults.task_instance_heartbeat_interval(worker_node_parser)
        ParserDefaults.heartbeat_report_by_buffer(worker_node_parser)
        ParserDefaults.web_service_fqdn(worker_node_parser)
        ParserDefaults.web_service_port(worker_node_parser)


def main(argstr: Optional[str] = None) -> None:
    """Entrypoint to create WorkerNode CLI."""
    cli = WorkerNodeCLI()
    cli.main(argstr)
