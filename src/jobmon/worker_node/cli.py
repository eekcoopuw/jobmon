"""Command line interface for Execution."""
from typing import Optional

import configargparse

from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults
from jobmon.exceptions import ReturnCodes
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


class WorkerNodeCLI(CLI):
    """Command line interface for WorkderNode."""

    def __init__(self) -> None:
        """Initialization of the worker node CLI."""
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=configargparse.ArgumentParser
        )

        self._add_worker_node_parser()

    def run_task(self, args: configargparse.Namespace) -> ReturnCodes:
        """Configuration for the jobmon worker node."""
        from jobmon.worker_node.worker_node_config import WorkerNodeConfig

        worker_node_config = WorkerNodeConfig(
            task_instance_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port,
        )

        worker_node_task_instance = WorkerNodeTaskInstance(
            task_instance_id=args.task_instance_id,
            array_id=args.array_id,
            batch_num=args.batch_number,
            expected_jobmon_version=args.expected_jobmon_version,
            cluster_type_name=args.cluster_type_name,
            requester_url=worker_node_config.url,
        )

        return worker_node_task_instance.run(
            heartbeat_interval=worker_node_config.task_instance_heartbeat_interval,
            report_by_buffer=worker_node_config.heartbeat_report_by_buffer,
        )

    def _add_worker_node_parser(self) -> None:
        worker_node_parser = self._subparsers.add_parser("worker_node", **PARSER_KWARGS)
        worker_node_parser.set_defaults(func=self.run_task)
        worker_node_parser.add_argument(
            "--task_instance_id",
            type=int,
            help="task_instance_id of the work node.",
            required=False,
        )
        worker_node_parser.add_argument(
            "--array_id",
            type=int,
            help="array_id of the worker node if this is an array task.",
            required=False
        )
        worker_node_parser.add_argument(
            "--batch_num",
            type=int,
            help="batch number of the array this task instance is associated with.",
            required=False
        )
        worker_node_parser.add_argument(
            "--cluster_type_name",
            type=str,
            help="cluster_type_name of the work node.",
            required=True,
        )
        worker_node_parser.add_argument(
            "--expected_jobmon_version",
            type=str,
            help="expected_jobmon_version of the work node.",
            required=True,
        )
        ParserDefaults.task_instance_heartbeat_interval(worker_node_parser)
        ParserDefaults.heartbeat_report_by_buffer(worker_node_parser)
        ParserDefaults.web_service_fqdn(worker_node_parser)
        ParserDefaults.web_service_port(worker_node_parser)


def run(argstr: Optional[str] = None) -> ReturnCodes:
    """Entrypoint to create WorkerNode CLI."""
    cli = WorkerNodeCLI()
    args = cli.parse_args(argstr)
    return cli.run_task(args)
