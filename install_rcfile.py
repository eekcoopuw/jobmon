import argparse
import json
import os
import shutil
import socket


if __name__ == "__main__":

    parser = argparse.ArgumentParser("Installs jobmon rc file")
    parser.add_argument("-f", "--force", action='store_true')
    args = parser.parse_args()

    rcfile = os.path.abspath(os.path.expanduser("~/.jobmonrc"))
    if os.path.exists(rcfile):
        if not args.force:
            raise FileExistsError("rcfile already exists. Use -f/--force if "
                                  "you want to overwrite it. The existing "
                                  "file will be backed up to "
                                  "~/.jobmonrc.backup")
        backup_file = "{}.backup".format(rcfile)
        shutil.move(rcfile, backup_file)

    with open(rcfile, "w") as jf:
        cfg_dct = {
            "conn_str": "sqlite://",
            "host": socket.gethostname(),
            "jsm_rep_port": 3456,
            "jsm_pub_port": 3457,
            "jqs_port": 3458}
        json.dump(cfg_dct, jf)
