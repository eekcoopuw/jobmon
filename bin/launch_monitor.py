#!/usr/bin/env python
import sys
from jobmon import monitor


if __name__ == '__main__':
    dir_path = sys.argv[1]

    m = monitor.JobMonitor(dir_path)
    m.run()
