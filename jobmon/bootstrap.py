import os
import shutil
from datetime import datetime
import json


def install_rcfile(args, cfg_dct=None):
    rcfile = os.path.abspath(os.path.expanduser(args.file))
    if os.path.exists(rcfile):
        if not args.force:
            raise FileExistsError("rcfile already exists. Use -f/--force if "
                                  "you want to overwrite it. The existing "
                                  "file will be backed up to "
                                  "{}.backup-{{datetime}}".format(rcfile))
        now = datetime.now().strftime("%m%d%Y-%H%M%S")
        backup_file = "{}.backup-{}".format(rcfile, now)
        shutil.move(rcfile, backup_file)

    with open(rcfile, "w") as jf:
        if not cfg_dct:
            conn_str = ("mysql://docker:docker@"
                        "jobmon-p01.ihme.washington.edu:3312/docker")
            cfg_dct = {
                "conn_str": conn_str,
                "host": "jobmon-p01.ihme.washington.edu",
                "jsm_rep_port": 5056,
                "jsm_pub_port": 5057,
                "jqs_port": 5058}
        json.dump(cfg_dct, jf)
