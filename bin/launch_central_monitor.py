#!/usr/bin/env python

# Tiny wrapper script to launch the CentralJobMonitor (in its own python process)

import sys
from jobmon import central_job_monitor


if __name__ == '__main__':
    dir_path = sys.argv[1]
    port = sys.argv[2]

    central_job_monitor.CentralJobMonitor(dir_path, port=port)
    # Creating that object also starts it
