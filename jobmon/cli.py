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
        self._add_workflow_jobs_subparser()
        self._add_job_status_subparser()

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
        if args.service == "health_monitor":
            from jobmon.server.start import start_health_monitor
            start_health_monitor()
        if args.service == "web_service":
            from jobmon.server.start import start_uwsgi_based_web_service
            start_uwsgi_based_web_service()
        else:
            raise ValueError("Only health_monitor or web_service can be "
                             "'started'. Got {}".format(args.service))

    def test_connection(self, args):
        from jobmon.client import shared_requester

        # check the server's is_alive? route
        shared_requester.send_request(app_route='/', request_type='get')

    def workflow_status(self, args):
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_status
        df = workflow_status(args.workflow_id, args.user)
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def workflow_jobs(self, args):
        from tabulate import tabulate
        from jobmon.client.status_commands import workflow_jobs
        df = workflow_jobs(args.workflow_id, args.status)
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def job_status(self, args):
        from tabulate import tabulate
        from jobmon.client.status_commands import job_status
        job_state, df = job_status(args.job_id)
        print(f"\nJOB_ID: {args.job_id}", f" STATUS: {job_state}\n")
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    def _add_start_subparser(self):
        start_parser = self._subparsers.add_parser("start")
        start_parser.set_defaults(func=self.start)
        start_parser.add_argument("service",
                                  choices=['health_monitor', 'web_service'])

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

    def _add_workflow_jobs_subparser(self):
        workflow_jobs_parser = self._subparsers.add_parser("workflow_jobs")
        workflow_jobs_parser.set_defaults(func=self.workflow_jobs)
        workflow_jobs_parser.add_argument(
            "-w", "--workflow_id", help="workflow_id to get job statuses for",
            required=True, type=int)
        workflow_jobs_parser.add_argument(
            "-s", "--status",
            help="limit jobs to a status (PENDING, RUNNING, DONE, FATAL)",
            choices=["PENDING", "RUNNING", "DONE", "FATAL",
                     "pending", "running", "done", "fatal"],
            required=False)

    def _add_job_status_subparser(self):
        job_status_parser = self._subparsers.add_parser("job_status")
        job_status_parser.set_defaults(func=self.job_status)
        job_status_parser.add_argument(
            "-j", "--job_id", help="job_id to get job statuses for",
            required=True, type=int)


def main():
    cli = CLI()
    cli.main()


if __name__ == "__main__":
    main()
