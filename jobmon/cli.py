import logging
import argparse

from jobmon import config
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
    jsm = JobStateManager(args.state_manager_reply_port,
                          args.state_manager_publish_port)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    jsm.open_socket()
    jsm.listen()


if __name__ == "__main__":
    main()
