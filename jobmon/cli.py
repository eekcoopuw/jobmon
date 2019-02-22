import argparse
import shlex
import subprocess

from jobmon.client import shared_requester
from jobmon.models import DB, database_loaders
from jobmon.server import create_app, ServerConfig
from jobmon.server.health_monitor.notifiers import SlackNotifier
from jobmon.server.health_monitor.health_monitor import HealthMonitor

try:
    FileExistsError
except NameError:
    FileExistsError = IOError


class CLI(object):

    def __init__(self):
        self.config = ServerConfig.from_defaults()

        # Create base parser and subparsers
        self.parser = argparse.ArgumentParser(description="Jobmon")
        subparsers = self.parser.add_subparsers(dest="sub_command")

        initdb_parser = subparsers.add_parser("initdb")
        initdb_parser.set_defaults(func=self.initdb)

        start_parser = subparsers.add_parser("start")
        start_parser.set_defaults(func=self.start)
        start_parser.add_argument("service",
                                  choices=['health_monitor', 'web_service'])

        test_parser = subparsers.add_parser("test")
        test_parser.set_defaults(func=self.test_connection)

    def main(self):
        args = self.parse_args()
        args.func(args)

    def initdb(self, args):
        """Create the database tables and load them with the requisite
        Job and JobInstance statuses
        """

        app = create_app()
        DB.init_app(app)
        with app.app_context():
            database_loaders.main(DB)

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
                             "{initdb, start, test}")
        return args

    def start(self, args):
        """Start the monitoring service"""
        if args.service == "health_monitor":
            self.start_health_monitor()
        if args.service == "web_service":
            self.start_uwsgi_based_web_service()
        else:
            raise ValueError("Only health_monitor or web_service can be "
                             "'started'. Got {}".format(args.service))

    def start_health_monitor(self):
        """Start monitoring for lost workflow runs"""
        if self.config.slack_token:
            wf_notifier = SlackNotifier(
                self.cconfig.slack_token,
                self.config.default_wf_slack_channel)
            wf_sink = wf_notifier.send
            node_notifier = SlackNotifier(
                self.config.slack_token,
                self.config.default_node_slack_channel)
            node_sink = node_notifier.send
        else:
            wf_sink = None
            node_sink = None
        hm = HealthMonitor(wf_notification_sink=wf_sink,
                           node_notification_sink=node_sink)
        hm.monitor_forever()

    def start_uwsgi_based_web_service(self):
        subprocess.run("/entrypoint.sh")
        subprocess.run("/start.sh")

    def test_connection(self, args):
        # check the server's is_alive? route
        shared_requester.send_request(app_route='/', request_type='get')


def main():
    cli = CLI()
    cli.main()


if __name__ == "__main__":
    main()
