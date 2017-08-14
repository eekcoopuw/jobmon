#!/usr/bin/env python

# Tiny wrapper script to launch the CentralJobMonitor (in its own python
# process)

import argparse

from jobmon import central_job_monitor


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Launch job monitoring process')
    parser.add_argument(
        '--dir_path', type=str,
        help='directory where monitor connection info will be stored')
    parser.add_argument(
        '--mon_port', nargs='?', default=None,
        help='monitor port to listen on (will be randomly assigned if absent)')
    parser.add_argument(
        '--pub_port', nargs='?', default=None,
        help='publisher port (will be randomly assigned if absent)')
    parser.add_argument(
        '--conn_str', type=str, default=None,
        help=('connection string for sql database (defaults to creation '
              'a sqlite db in dir_path)'))
    parser.add_argument(
        '--persistent', action='store_true', default=False,
        help='if true, spin up a local sqlite database')
    args = parser.parse_args()

    dir_path = args.dir_path
    mon_port = args.mon_port
    pub_port = args.pub_port
    conn_str = args.conn_str
    persistent = args.persistent

    # Creating this object also starts the monitor process
    cjm = central_job_monitor.CentralJobMonitor(
        dir_path,
        port=mon_port,
        conn_str=conn_str,
        publisher_port=pub_port,
        persistent=persistent)
