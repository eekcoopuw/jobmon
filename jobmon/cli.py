import argparse
import importlib
import logging

from jobmon import database
from jobmon.config import config
from jobmon.job_state_manager import JobStateManager


def main():
    parser = argparse.ArgumentParser(description="Jobmon")
    parser.add_argument("--state_manager_reply_port", type=int)
    parser.add_argument("--state_manager_publish_port", type=int)
    parser.add_argument("--db_host", type=str, default="db")
    parser.add_argument("--db_database", type=str, default="docker")
    parser.add_argument("--db_user", type=str, default="docker")
    parser.add_argument("--db_password", type=str, default="docker")
    parser.add_argument("-v", "--verbose", action='store_true')

    args = parser.parse_args()
    config.conn_str = "mysql://{user}:{passw}@{dbhost}/{dbdb}".format(
        user=args.db_user, passw=args.db_password, dbhost=args.db_host,
        dbdb=args.db_database)
    importlib.reload(database)  # Need to propagate this conn_str change

    database.create_job_db()
    try:
        with database.session_scope() as session:
            database.load_default_statuses(session)
    except:
        # This will fail if we've already loaded statuses (which will likely
        # be the case if we're using the same db for multiple tests)
        pass
    jsm = JobStateManager(args.state_manager_reply_port,
                          args.state_manager_publish_port)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    jsm.open_socket()
    jsm.listen()
