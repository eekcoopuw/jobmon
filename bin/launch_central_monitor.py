#!/usr/bin/env python

# Tiny wrapper script to launch the CentralJobMonitor (in its own python
# process)

import argparse

from jobmon import central_job_monitor


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Launch job monitoring process')
    parser.add_argument(
        'dir_path', type=str,
        help='directory where monitor connection info will be stored')
    parser.add_argument(
        'port', nargs='?', default=None,
        help='port to listen on (will be randomly assigned if absent)')
    parser.add_argument(
        '--conn_str', type=str, default=None,
        help=('connection string for sql database (defaults to creation '
              'a sqlite db in dir_path)'))
    args = parser.parse_args()

    dir_path = args.dir_path
    port = args.port
    conn_str = args.conn_str

    # Creating this object also starts the monitor process
    cjm = central_job_monitor.CentralJobMonitor(dir_path, port=port,
                                                conn_str=conn_str, persistent=False)
