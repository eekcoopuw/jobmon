"""Client command line interface for workflow/task status and concurrency limiting."""
import argparse
import json
from typing import Optional

import configargparse

from jobmon.client.client_config import ClientConfig
from jobmon.config import CLI, PARSER_KWARGS, ParserDefaults


class _HelpAction(argparse._HelpAction):
    """To show help for all subparsers in one place."""

    def __call__(self, parser, namespace, values, option_string=None):
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
        self.parser = configargparse.ArgumentParser(add_help=False, **PARSER_KWARGS)
        self.parser.add_argument('--help', action=_HelpAction, help="Help if you need Help")
        self._subparsers = self.parser.add_subparsers(
            dest='sub_command', parser_class=configargparse.ArgumentParser)

        self._add_workflow_status_subparser()
        self._add_workflow_tasks_subparser()
        self._add_task_template_resources_subparser()
        self._add_task_status_subparser()
        self._add_update_task_status_subparser()
        self._add_concurrency_limit_subparser()
        self._add_version_subparser()
        self._add_task_dependencies_subparser()
        self._add_workflow_reset_subparser()
        self._add_create_resource_yaml_subparser()

    @staticmethod
    def workflow_status(args: configargparse.Namespace) -> None:
        """Workflow status checking options."""
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_status as workflow_status_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)

        # Return the workflow_status data frame
        # If limit is specified along with multiple workflow_ids, set limit to the number of
        # workflows.
        limit = max(args.limit, len(args.workflow_id))
        df = workflow_status_cmd(args.workflow_id,
                                 args.user,
                                 args.json,
                                 cc.url,
                                 limit)

        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @staticmethod
    def workflow_tasks(args: configargparse.Namespace) -> None:
        """Check the tasks for a given workflow."""
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_tasks as workflow_tasks_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        df = workflow_tasks_cmd(args.workflow_id, args.status, args.json, cc.url)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    @staticmethod
    def task_template_resources(args: configargparse.Namespace) -> None:
        """Aggregates the resource usage for a given TaskTemplateVersion."""
        from jobmon.client.status_commands import task_template_resources

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        resources = task_template_resources(args.task_template_version, args.workflows,
                                            args.node_args, cc.url)
        print(resources)

    @staticmethod
    def task_status(args: configargparse.Namespace) -> None:
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

    @staticmethod
    def update_task_status(args: configargparse.Namespace) -> None:
        """Manually update task status for resumes, reruns, etc."""
        from jobmon.client.status_commands import update_task_status

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        response = update_task_status(args.task_ids, args.workflow_id, args.new_status,
                                      args.force, args.recursive, cc.url)
        print(f"Response is: {response}")

    @staticmethod
    def concurrency_limit(args: configargparse.Namespace) -> None:
        """Set a limit for the number of tasks that can run concurrently."""
        from jobmon.client.status_commands import concurrency_limit as concurrency_limit_cmd

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        response = concurrency_limit_cmd(args.workflow_id, args.max_tasks, cc.url)
        print(response)

    @staticmethod
    def task_dependencies(args: configargparse.Namespace) -> None:
        """Get task's upstream and downstream tasks and their status"""
        from jobmon.client.status_commands import get_task_dependencies
        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        r = get_task_dependencies(args.task_id, cc.url)
        up = r["up"]
        down = r["down"]
        """Format output that should look like:
        Upstream Tasks:

             Task ID         Status
             1               D

        Downstream Tasks:

            Task ID         Status
            3               D
            4               D
        """
        print('Upstream Tasks:\n')
        print("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in up:
            task_id = item['id']
            status = item['status']
            print("{:<8} {:<15} {:<15}".format("", task_id, status))
        print('\nDownstream Tasks:\n')
        print("{:<8} {:<15} {:<15}".format("", "Task ID", "Status"))
        for item in down:
            task_id = item['id']
            status = item['status']
            print("{:<8} {:<15} {:<15}".format("", task_id, status))

    @staticmethod
    def workflow_reset(args: configargparse.Namespace) -> None:
        """Manually reset a workflow."""
        from jobmon.client.status_commands import workflow_reset

        cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
        response = workflow_reset(args.workflow_id, cc.url)
        print(f"Response is: {response}")

    @staticmethod
    def jobmon_version(args: configargparse.Namespace) -> None:
        """Return the jobmon version"""
        from jobmon import _version
        print(_version.version)

    @staticmethod
    def resource_yaml(args: configargparse.Namespace) -> None:
        """Create resource yaml"""
        from jobmon.client.status_commands import create_resource_yaml
        # input check
        if (args.workflow_id is None) ^ (args.task_id is None):
            cc = ClientConfig(args.web_service_fqdn, args.web_service_port)
            r = create_resource_yaml(args.workflow_id, args.task_id, args.value_mem,
                                     args.value_core, args.value_runtime, args.clusters,
                                     cc.url)
            if args.print:
                print(r)
            if args.file:
                f = open(args.file, "w")
                f.write(r)
                f.close()
        else:
            print("Please provide a value for either -w or -t but not both.")

    def _add_version_subparser(self) -> None:
        version_parser = self._subparsers.add_parser("version", **PARSER_KWARGS)
        version_parser.set_defaults(func=self.jobmon_version)

    def _add_workflow_status_subparser(self) -> None:
        workflow_status_parser = self._subparsers.add_parser("workflow_status",
                                                             **PARSER_KWARGS)
        workflow_status_parser.set_defaults(func=self.workflow_status)
        workflow_status_parser.add_argument(
            "-w", "--workflow_id", nargs="*", help="list of workflow_ids", required=False,
            type=int, action='append', default=[]
        )
        workflow_status_parser.add_argument(
            "-u", "--user", nargs="*", help="list of users", required=False, type=str
        )
        workflow_status_parser.add_argument("-n", "--json", dest="json", action="store_true")
        workflow_status_parser.add_argument(
            "-l", "--limit",
            help="Limit the number of returning records. Default is 5.",
            required=False,
            default=5,
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
        ParserDefaults.web_service_fqdn(workflow_tasks_parser)
        ParserDefaults.web_service_port(workflow_tasks_parser)

    def _add_task_template_resources_subparser(self) -> None:
        tt_resources_parser = self._subparsers.add_parser("task_template_resources",
                                                          **PARSER_KWARGS)
        tt_resources_parser.set_defaults(func=self.task_template_resources)
        tt_resources_parser.add_argument(
            "-t", "--task_template_version",
            help="TaskTemplateVersion ID to get resource usage for",
            required=True, type=int
        )
        tt_resources_parser.add_argument(
            "-w", "--workflows", nargs="*", help="list of workflow IDs to query by",
            required=False, type=int
        )
        tt_resources_parser.add_argument(
            "-a", "--node_args", help="dictionary of node arguments to query by",
            required=False, type=json.loads
        )
        ParserDefaults.web_service_fqdn(tt_resources_parser)
        ParserDefaults.web_service_port(tt_resources_parser)

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
            "-s", "--new_status",
            help="Status to set to. \"D\" for DONE; \"G\" for REGISTERED/(pending).",
            choices=["D", "G"], type=str)
        update_task_parser.add_argument(
            "-f", "--force",
            help="Allow any of current task statuses, and any of the workflow statuses",
            required=False, action="store_true")
        update_task_parser.add_argument(
            "-r", "--recursive",
            help="Recursve update_task_status upstream or downstream depending "
                 "on -s, and on condition of -f",
            required=False, action="store_true")
        ParserDefaults.web_service_fqdn(update_task_parser)
        ParserDefaults.web_service_port(update_task_parser)

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
        def _validate_ntasks(x):
            try:
                x = int(x)
            except ValueError:
                raise argparse.ArgumentError(f"{x} is not coercible to an integer.")
            if x < 0:
                raise argparse.ArgumentError("Max concurrent tasks must be at least 0")
            return x

        concurrency_limit_parser.add_argument(
            "-m", "--max_tasks",
            required=True,
            type=_validate_ntasks,
            help="Number of concurrent tasks to allow. Must be at least 1.")
        ParserDefaults.web_service_fqdn(concurrency_limit_parser)
        ParserDefaults.web_service_port(concurrency_limit_parser)

    def _add_task_dependencies_subparser(self) -> None:
        task_dependencies_parser = self._subparsers.add_parser("task_dependencies",
                                                               **PARSER_KWARGS)
        task_dependencies_parser.set_defaults(func=self.task_dependencies)
        task_dependencies_parser.add_argument(
            "-t", "--task_id",
            help="list of task dependencies",
            required=True,
            type=int)
        ParserDefaults.web_service_fqdn(task_dependencies_parser)
        ParserDefaults.web_service_port(task_dependencies_parser)

    def _add_workflow_reset_subparser(self) -> None:
        workflow_reset_parser = \
            self._subparsers.add_parser("workflow_reset", **PARSER_KWARGS)
        workflow_reset_parser.set_defaults(func=self.workflow_reset)
        workflow_reset_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id to reset",
            required=True, type=int)
        ParserDefaults.web_service_fqdn(workflow_reset_parser)
        ParserDefaults.web_service_port(workflow_reset_parser)

    def _add_create_resource_yaml_subparser(self) -> None:
        create_resource_yaml_parser = \
            self._subparsers.add_parser("create_resource_yaml", **PARSER_KWARGS)
        create_resource_yaml_parser.set_defaults(func=self.resource_yaml)
        create_resource_yaml_parser.add_argument(
            "-w", "--workflow_id", help="The workflow id to generate resource YAML. "
                                        "Must provide either -w or -t.",
            required=False, type=int)
        create_resource_yaml_parser.add_argument(
            "-t", "--task_id", help="The workflow id to generate resource YAML. "
                                    "Must provide either -w or -t.",
            required=False, type=int)
        create_resource_yaml_parser.add_argument(
            "--value_mem", help="The algorithm to get memory usage. Default avg.",
            choices=["avg", "max", "min"], required=False, default="avg", type=str)
        create_resource_yaml_parser.add_argument(
            "--value_core", help="The algorithm to get core requested. Default avg.",
            choices=["avg", "max", "min"], required=False, default="avg", type=str)
        create_resource_yaml_parser.add_argument(
            "--value_runtime", help="The algorithm to get runtime. Default max.",
            choices=["avg", "max", "min"], required=False, default="max", type=str)
        create_resource_yaml_parser.add_argument(
            "-f", "--file", help="The file to save the YAML.",
            required=False, default=None, type=str)
        create_resource_yaml_parser.add_argument(
            "-p", "--print", help="Print the result YAMl to standard output.",
            required=False, default=False, action="store_true")
        create_resource_yaml_parser.add_argument(
            "-c", "--clusters", nargs="+", help="The clusters for the YAML.",
            required=False, default=["ihme_slurm"], type=str)
        ParserDefaults.web_service_fqdn(create_resource_yaml_parser)
        ParserDefaults.web_service_port(create_resource_yaml_parser)


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ClientCLI()
    cli.main(argstr)
