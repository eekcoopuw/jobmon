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
                        "jobmon-p01.ihme.washington.edu:3315/docker")
            cfg_dct = {
                "conn_str": conn_str,
                "host": "jobmon-p01.ihme.washington.edu",
                "jsm_port": 6256,
                "jqs_port": 6258}
        json.dump(cfg_dct, jf)
