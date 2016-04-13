import sys
import os
import traceback
import argparse
this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))
sys.path.append(this_dir + "/..")
from jobmon import job

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--mon_dir", required=True)
parser.add_argument("--runfile", required=True)
parser.add_argument("--jid", required=True, type=int)
parser.add_argument('pass_through', nargs='*')
args = vars(parser.parse_args())

# reset sys.argv as if this parsing never happened
passed_params = []
for param in args["pass_through"]:
    passed_params.append(str(param).replace("##", "--", 1))
sys.argv = [args["runfile"]] + passed_params

#
j1 = job.Job(args["mon_dir"], jid=args["jid"])
try:
    j1.start()
    execfile(args["runfile"])
except:
    tb = traceback.format_exc()
    j1.log_error(tb)
    j1.failed()
else:
    j1.finish()
