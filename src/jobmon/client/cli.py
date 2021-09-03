"""Client command line interface for workflow/task status and concurrency limiting."""
import argparse
from typing import Any, Optional

import configargparse

from jobmon.client.client_config import ClientConfig
from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults


class _HelpAction(argparse._HelpAction):
    """To show help for all subparsers in one place."""

    def __call__(self, parser: Any, namespace: argparse.Namespace, values: Any,
                 option_string: str = None) -> None:
        """Add subparsers' help info when jobmon --help is called."""
        print(parser.format_help())
        subparsers_actions = [action for action in parser._actions if
                              isinstance(action, argparse._SubParsersAction)]
        print("Jobmon Usage Options:")
        for sub_action in subparsers_actions:
            for choice, subparser in sub_action.choices.items():
                print(f"{choice.upper()} (for more information specify 'jobmon {choice} "
                      f"--help'):")
                print(subparser.format_usage())
        parser.exit()


class ClientCLI(CLI):
    """Client command line interface for workflow/task status and concurrency limiting."""

    def __init__(self) -> None:
        """Initialization of client CLI."""
        self.parser = configargparse.ArgumentParser(add_help=False, **PARSER_KWARGS)
        self.parser.add_argument('--help', action=_HelpAction, help="Help if you need Help")
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser)

        self._add_workflow_status_subparser()
        self._add_workflow_tasks_subparser()
        self._add_task_status_subparser()
        self._add_update_task_status_subparser()
        self._add_update_config_subparser()
        self._add_concurrency_limit_subparser()

    def workflow_status(self, args: configargparse.Namespace) -> None:
        """Workflow status checking options."""
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_status as workflow_status_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        limit = args.limit if args.limit is None or args.limit > 0 else -1
        df = workflow_status_cmd(args.workflow_id, args.user, args.json, cc.url, limit)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def workflow_tasks(self, args: configargparse.Namespace) -> None:
        """Check the tasks for a given workflow."""
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_tasks as workflow_tasks_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        limit = args.limit if args.limit is None or args.limit > 0 else -1
        df = workflow_tasks_cmd(args.workflow_id, args.status, args.json, cc.url, limit)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def task_status(self, args: configargparse.Namespace) -> None:
        """Check task status."""
        from tabulate import tabulate
        from jobmon.client.status_commands import task_status as task_status_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        df = task_status_cmd(args.task_ids, args.status, args.json, cc.url)
        print(f"\nTASK_IDS: {args.task_ids}")
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def update_task_status(self, args: configargparse.Namespace) -> None:
        """Manually update task status for resumes, reruns, etc."""
        from jobmon.client.status_commands import update_task_status

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        response = update_task_status(args.task_ids, args.workflow_id, args.new_status, cc.url)
        print(f"Response is: {response}")

    def update_config(self, args: configargparse.Namespace) -> None:
        """Update .jobmon.ini.

        Args:
            args: only --web_service_fqdn --web_service_port are expected.
        """
        from jobmon.client.status_commands import update_config
        import requests

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)

        # validate the updated url is reachable
        try:
            _ = requests.get(cc.url)
        except requests.ConnectionError:
            raise AssertionError(f"URL {cc.url} is not reachable.")

        update_config(cc)

    def concurrency_limit(self, args: configargparse.Namespace) -> None:
        """Set a limit for the number of tasks that can run concurrently."""
        from jobmon.client.status_commands import concurrency_limit as concurrency_limit_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        response = concurrency_limit_cmd(args.workflow_id, args.max_tasks, cc.url)
        print(response)

    def _add_workflow_status_subparser(self) -> None:
        workflow_status_parser = self._subparsers.add_parser("workflow_status",
                                                             **PARSER_KWARGS)
        workflow_status_parser.set_defaults(func=self.workflow_status)
        workflow_status_parser.add_argument(
            "-w", "--workflow_id", nargs="*", help="list of workflow_ids", required=False,
            type=int
        )
        workflow_status_parser.add_argument(
            "-u", "--user", nargs="*", help="list of users", required=False, type=str
        )
        workflow_status_parser.add_argument("-n", "--json", dest="json", action="store_true")
        workflow_status_parser.add_argument(
            "-l", "--limit",
            nargs="*",
            default=5,
            help="limit the number of returning records; default is 5",
            required=False,
            type=int
        )
        ParserDefaults.web_service_fqdn(workflow_status_parser)
        ParserDefaults.web_service_port(workflow_status_parser)

    def _add_workflow_tasks_subparser(self) -> None:
        workflow_tasks_parser = self._subparsers.add_parser("workflow_tasks", **PARSER_KWARGS)
        workflow_tasks_parser.set_defaults(func=self.workflow_tasks)
        workflow_tasks_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id to get task statuses for",
            required=True, type=int
        )
        workflow_tasks_parser.add_argument(
            "-s", "--status", nargs="*",
            help="limit tasks to a status (PENDING, RUNNING, DONE, FATAL)",
            choices=["PENDING", "RUNNING", "DONE", "FATAL",
                     "pending", "running", "done", "fatal"],
            required=False
        )
        workflow_tasks_parser.add_argument("-n", "--json", dest="json", action="store_true")
        workflow_tasks_parser.add_argument(
            "-l", "--limit",
            nargs="*",
            default=5,
            help="limit the number of returning records; default is 5",
            required=False,
            type=int
        )
        ParserDefaults.web_service_fqdn(workflow_tasks_parser)
        ParserDefaults.web_service_port(workflow_tasks_parser)

    def _add_task_status_subparser(self) -> None:
        task_status_parser = self._subparsers.add_parser("task_status", **PARSER_KWARGS)
        task_status_parser.set_defaults(func=self.task_status)
        task_status_parser.add_argument(
            "-t", "--task_ids", nargs="+", help="task_ids to get task statuses for",
            required=True, type=int)
        task_status_parser.add_argument(
            "-s", "--status", nargs="*",
            help="limit task instances to statuses (PENDING, RUNNING, DONE, FATAL)",
            choices=["PENDING", "RUNNING", "DONE", "FATAL",
                     "pending", "running", "done", "fatal"],
            required=False)
        task_status_parser.add_argument("-n", "--json", dest="json", action="store_true")
        ParserDefaults.web_service_fqdn(task_status_parser)
        ParserDefaults.web_service_port(task_status_parser)

    def _add_update_task_status_subparser(self) -> None:
        update_task_parser = self._subparsers.add_parser("update_task_status", **PARSER_KWARGS)
        update_task_parser.set_defaults(func=self.update_task_status)
        update_task_parser.add_argument(
            "-t", "--task_ids", nargs="+", help="task_ids to reset",
            required=True, type=int)
        update_task_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id of the tasks to reset",
            required=True, type=int)
        update_task_parser.add_argument(
            "-s", "--new_status", help="status to set to",
            choices=["D", "G"], type=str)
        ParserDefaults.web_service_fqdn(update_task_parser)
        ParserDefaults.web_service_port(update_task_parser)

    def _add_update_config_subparser(self) -> None:
        parser_kwargs = dict(PARSER_KWARGS)
        parser_kwargs.pop('args_for_setting_config_path')
        update_config_parser = self._subparsers.add_parser("update_config", **parser_kwargs)
        update_config_parser.set_defaults(func=self.update_config)
        ParserDefaults.web_service_fqdn(update_config_parser)
        ParserDefaults.web_service_port(update_config_parser)

    def _add_concurrency_limit_subparser(self) -> None:
        concurrency_limit_parser = self._subparsers.add_parser("concurrency_limit",
                                                               **PARSER_KWARGS)
        concurrency_limit_parser.set_defaults(func=self.concurrency_limit)
        concurrency_limit_parser.add_argument(
            "-w", "--workflow_id",
            required=True,
            type=int,
            help="Workflow ID of the workflow to be adjusted")

        # Define a custom function to validate the user's input.
        def _validate_ntasks(x: Any) -> int:
            try:
                x = int(x)
            except ValueError:
                raise argparse.ArgumentTypeError(f"{x} is not coercible to an integer.")
            if x < 0:
                raise argparse.ArgumentTypeError("Max concurrent tasks must be at least 0.")
            return x

        concurrency_limit_parser.add_argument(
            "-m", "--max_tasks",
            required=True,
            type=_validate_ntasks,
            help="Number of concurrent tasks to allow. Must be at least 1.")
        ParserDefaults.web_service_fqdn(concurrency_limit_parser)
        ParserDefaults.web_service_port(concurrency_limit_parser)


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ClientCLI()
    cli.main(argstr)
