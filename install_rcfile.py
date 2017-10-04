import json
import os
import shutil
import socket


rcfile = os.path.abspath(os.path.expanduser("~/.jobmonrc"))
if os.path.exists(rcfile):
    backup_file = "{}.backup".format(rcfile)
    if os.path.exists(backup_file):
        raise FileExistsError("rcfile has already been replaced once. "
                              "if the contents of ~/.jobmonrc.backup are no "
                              "longer needed, please delete the file")
    shutil.move(rcfile, backup_file)

with open(rcfile, "w") as jf:
    cfg_dct = {
        "conn_str": "sqlite://",
        "host": socket.gethostname(),
        "jsm_rep_port": 3456,
        "jsm_pub_port": 3457,
        "jqs_port": 3458}
    json.dump(cfg_dct, jf)
