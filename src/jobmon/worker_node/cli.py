"""Command line interface for Execution."""
import logging
import sys
from typing import Optional

import configargparse

from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults

logger = logging.getLogger(__name__)


class WorkerNodeCLI(CLI):
    """Command line interface for WorkderNode."""

    def __init__(self) -> None:
        """Initialization of the worker node CLI."""
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=configargparse.ArgumentParser
        )

        self._add_worker_node_job_parser()
        self._add_worker_node_array_parser()

    def run_task_instance_job(self, args: configargparse.Namespace) -> int:
        """Configuration for the jobmon worker node."""
        from jobmon import __version__
        from jobmon.exceptions import ReturnCodes
        from jobmon.worker_node.worker_node_config import WorkerNodeConfig
        from jobmon.worker_node.worker_node_factory import WorkerNodeFactory

        if __version__ != args.expected_jobmon_version:
            msg = (
                f"Your expected Jobmon version is {args.expected_jobmon_version} and your "
                f"worker node is using {__version__}. Please check your bash profile "
            )
            logger.error(msg)
            sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

        worker_node_config = WorkerNodeConfig(
            task_instance_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port,
        )

        worker_node_factory = WorkerNodeFactory(
            cluster_name=args.cluster_name,
            worker_node_config=worker_node_config,
        )
        worker_node_task_instance = worker_node_factory.get_job_task_instance(
            task_instance_id=args.task_instance_id
        )
        worker_node_task_instance.configure_logging()
        try:
            worker_node_task_instance.run()
        except Exception as e:
            logger.error(e)
            sys.exit(ReturnCodes.WORKER_NODE_CLI_FAILURE)

        return worker_node_task_instance.command_return_code

    def run_task_instance_array(self, args: configargparse.Namespace) -> int:
        """Configuration for the jobmon worker node."""
        from jobmon import __version__
        from jobmon.exceptions import ReturnCodes
        from jobmon.worker_node.worker_node_config import WorkerNodeConfig
        from jobmon.worker_node.worker_node_factory import WorkerNodeFactory

        if __version__ != args.expected_jobmon_version:
            msg = (
                f"Your expected Jobmon version is {args.expected_jobmon_version} and your "
                f"worker node is using {__version__}. Please check your bash profile "
            )
            logger.error(msg)
            sys.exit(ReturnCodes.WORKER_NODE_ENV_FAILURE)

        worker_node_config = WorkerNodeConfig(
            task_instance_heartbeat_interval=args.task_instance_heartbeat_interval,
            heartbeat_report_by_buffer=args.heartbeat_report_by_buffer,
            web_service_fqdn=args.web_service_fqdn,
            web_service_port=args.web_service_port,
        )

        worker_node_factory = WorkerNodeFactory(
            cluster_name=args.cluster_name,
            worker_node_config=worker_node_config,
        )
        worker_node_task_instance = worker_node_factory.get_array_task_instance(
            array_id=args.array_id, batch_number=args.batch_number
        )
        worker_node_task_instance.configure_logging()

        try:
            worker_node_task_instance.run()
        except Exception as e:
            logger.error(e)
            sys.exit(ReturnCodes.WORKER_NODE_CLI_FAILURE)

        return worker_node_task_instance.command_return_code

    def _add_worker_node_job_parser(self) -> None:
        job_parser = self._subparsers.add_parser("worker_node_job", **PARSER_KWARGS)
        job_parser.set_defaults(func=self.run_task_instance_job)
        job_parser.add_argument(
            "--task_instance_id",
            help="task_instance_id of the work node.",
            required=False,
        )
        job_parser.add_argument(
            "--cluster_name",
            type=str,
            help="cluster_name of the work node.",
            required=True,
        )
        job_parser.add_argument(
            "--expected_jobmon_version",
            type=str,
            help="expected_jobmon_version of the work node.",
            required=True,
        )
        ParserDefaults.task_instance_heartbeat_interval(job_parser)
        ParserDefaults.heartbeat_report_by_buffer(job_parser)
        ParserDefaults.web_service_fqdn(job_parser)
        ParserDefaults.web_service_port(job_parser)

    def _add_worker_node_array_parser(self) -> None:
        array_parser = self._subparsers.add_parser("worker_node_array", **PARSER_KWARGS)
        array_parser.set_defaults(func=self.run_task_instance_array)
        array_parser.add_argument(
            "--array_id",
            help="array_id of the worker node if this is an array task.",
            required=False,
        )
        array_parser.add_argument(
            "--batch_number",
            help="batch number of the array this task instance is associated with.",
            required=False,
        )
        array_parser.add_argument(
            "--cluster_name",
            type=str,
            help="cluster_name of the work node.",
            required=True,
        )
        array_parser.add_argument(
            "--expected_jobmon_version",
            type=str,
            help="expected_jobmon_version of the work node.",
            required=True,
        )
        ParserDefaults.task_instance_heartbeat_interval(array_parser)
        ParserDefaults.heartbeat_report_by_buffer(array_parser)
        ParserDefaults.web_service_fqdn(array_parser)
        ParserDefaults.web_service_port(array_parser)


def run(argstr: Optional[str] = None) -> None:
    """Entrypoint to create WorkerNode CLI."""
    cli = WorkerNodeCLI()
    cli.main(argstr)
