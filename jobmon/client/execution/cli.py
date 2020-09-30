import configargparse

from typing import Optional


from jobmon.config import PARSER_KWARGS, ParserDefaults, CLI


class ExecutorCLI(CLI):

    def __init__(self) -> None:
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser
        )

    def scheduler(self, args: configargparse.Namespace) -> None:
        from jobmon.client.execution.scheduler.api import get_scheduler, SchedulerConfig
        from jobmon.client.client_config import ClientConfig

        requester_config = ClientConfig(args.web_service_host, args.web_service_port)
        scheduler_config = SchedulerConfig(
            jobmon_command=args.worker_node_entry_point,
            workflow_run_heartbeat_interval=args.workflow_run_heartbeat_interval,
            task_heartbeat_interval=args.task_instance_heartbeat_interval,
            report_by_buffer=args.task_instance_report_by_buffer
        )

        if args.command == 'start':
            scheduler = get_scheduler(args.workflow_id, args.workflow_run_id,
                                      requester_config.url, scheduler_config)
            scheduler.run_scheduler()
        else:
            raise ValueError(f"Command {args.command} not supported.")

    def _add_scheduler_parser(self) -> None:
        scheduler_parser = self._subparsers.add_parser('scheduler', **PARSER_KWARGS)
        scheduler_parser.set_defaults(func=self.scheduler)
        scheduler_parser.add_argument(
            'command',
            type=str,
            choices=['start'],
            help=('The web_server sub-command to run: (start, test). Start is not currently '
                  'supported. Test creates a test instance of the jobmon Flask app using the '
                  'Flask dev server and should not be used for production'),
            required=True
        )
        scheduler_parser.add_argument(
            'workflow_id',
            type=int,
            help='workflow_id to schedule jobs for.',
            required=True
        )
        scheduler_parser.add_argument(
            'workflow_run_id',
            type=int,
            help='workflow_run_id to schedule jobs for.',
            required=True
        )
        ParserDefaults.worker_node_entry_point(scheduler_parser)
        ParserDefaults.workflow_run_heartbeat_interval(scheduler_parser)
        ParserDefaults.task_instance_heartbeat_interval(scheduler_parser)
        ParserDefaults.task_instance_report_by_buffer(scheduler_parser)


def main(argstr: Optional[str] = None) -> None:
    cli = ExecutorCLI()
    cli.main(argstr)
