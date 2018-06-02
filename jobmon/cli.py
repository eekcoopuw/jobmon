import argparse
import logging
import shlex

from sqlalchemy.exc import IntegrityError

from jobmon import database
from jobmon import database_loaders
from jobmon import config
from jobmon.bootstrap import install_rcfile
from jobmon.database import session_scope
from jobmon.requester import Requester
from jobmon.notifiers import SlackNotifier
from jobmon.services.health_monitor import HealthMonitor
from jobmon.services.job_query_server import JobQueryServer
from jobmon.services.job_state_manager import JobStateManager

try:
    FileExistsError
except NameError:
    FileExistsError = IOError


def main():
    args = parse_args()
    if args.sub_command != "configure":
        apply_args_to_config(args)
    args.func(args)


def add_config_opts(parser):
    """Add the GlobalConfig options to the parser so they can
    override the .jobmonrc and default settings"""
    parser.add_argument("--config_file", type=str, default="~/.jobmonrc")
    for opt, default in config.GlobalConfig.default_opts.items():
        if isinstance(default, bool):
            parser.add_argument("--{}".format(opt), action='store_true')
        else:
            tp = type(default)
            parser.add_argument("--{}".format(opt), type=tp)
    return parser


def apply_args_to_config(args):
    """Override .jobmonrc and default settings with those passed
    via the command line"""
    cli_opts = vars(args)
    cli_opts = {k: v for k, v in cli_opts.items() if v is not None}
    config.config.apply_opts_dct(cli_opts)

    # Don't forget to recreate the engine... in case the conn_str in the
    # config has changed
    database.recreate_engine()
    return config.config


def initdb(args):
    """Create the database tables and load them with the requisite
    Job and JobInstance statuses"""
    database_loaders.create_job_db()
    try:
        with session_scope() as session:
            database_loaders.load_default_statuses(session)
    except IntegrityError as e:
        raise Exception("Database is not empty, "
                        "could not create tables {}").format(str(e))


def parse_args(argstr=None):
    """Constructs a parser, parses either sys.argv (default) or the provided
    argstr, returns a Namespace. The Namespace should have a 'func'
    attribute which can be used to dispatch to the appropriate downstream
    function"""
    parser = argparse.ArgumentParser(description="Jobmon")
    parser = add_config_opts(parser)

    # Create subparsers
    subparsers = parser.add_subparsers(dest="sub_command")

    config_parser = subparsers.add_parser(
        "configure", description="Installs jobmon rc file")
    config_parser.set_defaults(func=install_rcfile)
    config_parser.add_argument("-f", "--force", action='store_true')
    config_parser.add_argument("--file", type=str, default="~/.jobmonrc")

    initdb_parser = subparsers.add_parser("initdb")
    initdb_parser.set_defaults(func=initdb)

    start_parser = subparsers.add_parser("start")
    start_parser.set_defaults(func=start)
    start_parser.add_argument("service", choices=['job_state_manager',
                                                  'job_query_server',
                                                  'health_monitor'])

    test_parser = subparsers.add_parser("test")
    test_parser.set_defaults(func=test_connection)

    if argstr is not None:
        arglist = shlex.split(argstr)
        args = parser.parse_args(arglist)
    else:
        args = parser.parse_args()
    if not args.sub_command:
        raise ValueError("sub-command required: "
                         "{configure, initdb, start, test}")
    return args


def start(args):
    """Start the JobStateManager or JobQueryServer process listening"""
    if config.config.verbose:
        logging.basicConfig(level=logging.DEBUG)
    if args.service == "job_state_manager":
        start_job_state_manager()
    elif args.service == "job_query_server":
        start_job_query_server()
    elif args.service == "health_monitor":
        start_health_monitor()


def start_health_monitor():
    """Start monitoring for lost workflow runs"""

    if config.config.slack_token:
        wf_notifier = SlackNotifier(
            config.config.slack_token,
            config.config.default_wf_slack_channel)
        wf_sink = wf_notifier.send
        node_notifier = SlackNotifier(
            config.config.slack_token,
            config.config.default_node_slack_channel)
        node_sink = node_notifier.send
    else:
        wf_sink = None
        node_sink = None
    hm = HealthMonitor(wf_notification_sink=wf_sink,
                       node_notification_sink=node_sink)
    hm.monitor_forever()


def start_job_state_manager():
    """Start the JobStateManager process listening"""
    jsm = JobStateManager(config.config.jm_rep_conn.port,
                          config.config.jm_pub_conn.port)
    jsm.open_socket()
    jsm.listen()


def start_job_query_server():
    """Start the JobQueryServer process listening"""
    jqs = JobQueryServer(config.config.jqs_rep_conn.port)
    jqs.open_socket()
    jqs.listen()


def test_connection(args):
    jsm_req = Requester(config.jm_rep_conn)
    jsm_req.send_request({'action': 'alive'})
    jqs_req = Requester(config.jqs_rep_conn)
    jqs_req.send_request({'action': 'alive'})


if __name__ == "__main__":
    main()
