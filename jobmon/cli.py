import argparse
import logging
import json
import os
import shlex
import shutil
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from jobmon import database
from jobmon import config
from jobmon.requester import Requester
from jobmon.job_query_server import JobQueryServer
from jobmon.job_state_manager import JobStateManager


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


def install_rcfile(args):
    rcfile = os.path.abspath(os.path.expanduser(args.file))
    if os.path.exists(rcfile):
        if not args.force:
            raise FileExistsError("rcfile already exists. Use -f/--force if "
                                  "you want to overwrite it. The existing "
                                  "file will be backed up to "
                                  "{}.backup-{{datetime}}".format(rcfile))
        now = datetime.now().strftime("%m%d%Y-%H%M%S")
        backup_file = "{}.backup-{}".format(rcfile, now)
        shutil.move(rcfile, backup_file)

    with open(rcfile, "w") as jf:
        conn_str = ("mysql://docker:docker@"
                    "jobmon-p01.ihme.washington.edu/docker:3307")
        cfg_dct = {
            "conn_str": conn_str,
            "host": "jobmon-p01.ihme.washington.edu",
            "jsm_rep_port": 4556,
            "jsm_pub_port": 4557,
            "jqs_port": 4558}
        json.dump(cfg_dct, jf)


def initdb(args):
    """Create the database tables and load them with the requisite
    Job and JobInstance statuses"""
    database.create_job_db()
    try:
        with database.session_scope() as session:
            database.load_default_statuses(session)
    except IntegrityError as e:
        raise Exception("Database is not empty, "
                        "could not create tables") from e


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
                                                  'job_query_server'])

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
