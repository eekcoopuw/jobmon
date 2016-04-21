#!/usr/bin/env python
import sys
import traceback
import argparse
from jobmon import job

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--mon_dir", required=True)
parser.add_argument("--runfile", required=True)
parser.add_argument("--jid", required=True, type=int)
parser.add_argument("--request_timeout", required=False, type=int)
parser.add_argument("--request_retries", required=False, type=int)
parser.add_argument('pass_through', nargs='*')
args = vars(parser.parse_args())

# build kwargs list for optional stuff
kwargs = {}
if args["request_retries"] is not None:
    kwargs["request_retries"] = args["request_retries"]
if args["request_timeout"] is not None:
    kwargs["request_timeout"] = args["request_timeout"]

# reset sys.argv as if this parsing never happened
passed_params = []
for param in args["pass_through"]:
    passed_params.append(str(param).replace("##", "--", 1))
sys.argv = [args["runfile"]] + passed_params

#
j1 = job.Job(args["mon_dir"], jid=args["jid"], **kwargs)
try:
    j1.start()
    execfile(args["runfile"])
except:
    tb = traceback.format_exc()
    j1.log_error(tb)
    j1.failed()
else:
    j1.finish()
