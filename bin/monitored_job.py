#!/usr/bin/env python
from __future__ import print_function
import sys
import argparse
from subprocess import Popen, PIPE
from jobmon import job


# for sge logging of standard error
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

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

# start monitoring
j1 = job.Job(args["mon_dir"], jid=args["jid"], **kwargs)
j1.start()

# open subprocess
p = Popen(["python"] + sys.argv, stdout=PIPE, stderr=PIPE)
output, error = p.communicate()
if p.returncode != 0:

    # sge logging
    eprint(error)
    print(output)

    # jobmon logging
    j1.log_error(error)
    j1.failed()
else:
    j1.finish()
