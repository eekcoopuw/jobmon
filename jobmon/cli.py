import argparse
import shlex


class CLI(object):

    def __init__(self):

        # Create base parser
        self.parser = argparse.ArgumentParser(description="Jobmon")
        self._subparsers = self.parser.add_subparsers(dest="sub_command")

        # add subparsers
        self._add_start_subparser()
        self._add_test_subparser()
        self._add_workflow_status_subparser()
        self._add_workflow_tasks_subparser()
        self._add_task_status_subparser()

    def main(self):
        args = self.parse_args()
        args.func(args)

    def parse_args(self, argstr=None):
        """Construct a parser, parse either sys.argv (default) or the provided
        argstr, returns a Namespace. The Namespace should have a 'func'
        attribute which can be used to dispatch to the appropriate downstream
        function
        """
        if argstr is not None:
            arglist = shlex.split(argstr)
            args = self.parser.parse_args(arglist)
        else:
            args = self.parser.parse_args()
        if not args.sub_command:
            raise ValueError("sub-command required: "
                             "{start, test}")
        return args

    def start(self, args):
        """Start the monitoring service"""
        if args.service == "workflow_reaper":
            from jobmon.server.start import start_workflow_reaper
            start_workflow_reaper()
        if args.service == "qpid_integration":
            from jobmon.server.start import start_qpid_integration
            start_qpid_integration()
        if args.service == "web_service":
            from jobmon.server.start import start_uwsgi_based_web_service
            start_uwsgi_based_web_service()
        else:
            raise ValueError("Only workflow_reaper, qpid_integration, or web_service can be "
                             "'started'. Got {}".format(args.service))

    def test_connection(self, args):
        from jobmon.client.swarm import shared_requester

        # check the server's is_alive? route
        shared_requester.send_request(app_route='/', request_type='get')

    def workflow_status(self, args):
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_status
        df = workflow_status(args.workflow_id, args.user, args.json)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql",
                           showindex=False))

    def workflow_tasks(self, args):
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_tasks
        df = workflow_tasks(args.workflow_id, args.status, args.json)
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql",
                           showindex=False))

    def task_status(self, args):
        from tabulate import tabulate
        from jobmon.client.status_commands import task_status
        task_state, df = task_status(args.task_id, args.json)
        print(f"\nTASK_ID: {args.task_id}", f" STATUS: {task_state}\n")
        if args.json:
            print(df)
        else:
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def _add_start_subparser(self):
        start_parser = self._subparsers.add_parser("start")
        start_parser.set_defaults(func=self.start)
        start_parser.add_argument("service",
                                  choices=['workflow_reaper', 'web_service',
                                           'qpid_integration'])

    def _add_test_subparser(self):
        test_parser = self._subparsers.add_parser("test")
        test_parser.set_defaults(func=self.test_connection)

    def _add_workflow_status_subparser(self):
        workflow_status_parser = self._subparsers.add_parser("workflow_status")
        workflow_status_parser.set_defaults(func=self.workflow_status)
        workflow_status_parser.add_argument(
            "-w", "--workflow_id", nargs="*", help="list of workflow_ids",
            required=False, type=int)
        workflow_status_parser.add_argument(
            "-u", "--user", nargs="*", help="list of users",
            required=False, type=str)
        workflow_status_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )

    def _add_workflow_tasks_subparser(self):
        workflow_tasks_parser = self._subparsers.add_parser("workflow_tasks")
        workflow_tasks_parser.set_defaults(func=self.workflow_tasks)
        workflow_tasks_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id to get task statuses for",
            required=True, type=int)
        workflow_tasks_parser.add_argument(
            "-s", "--status",
            help="limit tasks to a status (PENDING, RUNNING, DONE, FATAL)",
            choices=["PENDING", "RUNNING", "DONE", "FATAL",
                     "pending", "running", "done", "fatal"],
            required=False)
        workflow_tasks_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )

    def _add_task_status_subparser(self):
        task_status_parser = self._subparsers.add_parser("task_status")
        task_status_parser.set_defaults(func=self.task_status)
        task_status_parser.add_argument(
            "-t", "--task_id", help="task_id to get task statuses for",
            required=True, type=int)
        task_status_parser.add_argument(
            "-n", "--json", dest="json", action="store_true"
        )


def main():
    cli = CLI()
    cli.main()


if __name__ == "__main__":
    main()
