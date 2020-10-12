import configargparse

from typing import Optional


from jobmon.config import PARSER_KWARGS, ParserDefaults, CLI


class ExecutorCLI(CLI):

    def __init__(self) -> None:
        self.parser = configargparse.ArgumentParser(**PARSER_KWARGS)
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser
        )

    def workflow_status(self, args) -> None:
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_status
        df = workflow_status(args.workflow_id, args.user, args.json)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def workflow_tasks(self, args) -> None:
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_tasks
        df = workflow_tasks(args.workflow_id, args.status, args.json)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def task_status(self, args) -> None:
        from tabulate import tabulate
        from jobmon.client.status_commands import task_status
        df = task_status(args.task_ids, args.status, args.json)
        print(f"\nTASK_IDS: {args.task_ids}")
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def _add_workflow_status_subparser(self) -> None:
        workflow_status_parser = self._subparsers.add_parser("workflow_status")
        workflow_status_parser.set_defaults(func=self.workflow_status)
        workflow_status_parser.add_argument(
            "-w", "--workflow_id", nargs="*", help="list of workflow_ids", required=False,
            type=int
        )
        workflow_status_parser.add_argument(
            "-u", "--user", nargs="*", help="list of users", required=False, type=str
        )
        workflow_status_parser.add_argument("-n", "--json", dest="json", action="store_true")
        ParserDefaults.web_service_fqdn(workflow_status_parser)
        ParserDefaults.web_service_port(workflow_status_parser)

    def _add_workflow_tasks_subparser(self) -> None:
        workflow_tasks_parser = self._subparsers.add_parser("workflow_tasks")
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
        ParserDefaults.web_service_fqdn(workflow_tasks_parser)
        ParserDefaults.web_service_port(workflow_tasks_parser)

    def _add_task_status_subparser(self) -> None:
        task_status_parser = self._subparsers.add_parser("task_status")
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
