#!/usr/bin/env python

# Tiuniy wrapper script to launch the CnetralJobMonitor (in its own python process)

import sys
from jobmon import central_job_monitor


if __name__ == '__main__':
    dir_path = sys.argv[1]

    m = central_job_monitor.CentralJobMonitor(dir_path)
    m.run()
