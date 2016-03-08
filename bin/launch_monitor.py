# Command line interface for starting a jobmon monitor
import sys
import os
this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))
sys.path.append(this_dir + "/../..")
from jobmon import monitor


if __name__ == '__main__':
    dir_path = sys.argv[1]

    m = monitor.JobMonitor(dir_path)
    m.run()
