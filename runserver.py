#!/usr/bin/env python

import getpass
import json
import os
import shutil
import subprocess


DEFAULT_SLACK_CHANNEL = 'jobmon-alerts'


if __name__ == "__main__":

    slack_token = getpass.getpass("Slack bot token: ") or None
    if slack_token:
        slack_channel = (
            input("Slack notificaiton channel "
                  "({}): ".format(DEFAULT_SLACK_CHANNEL)) or
            DEFAULT_SLACK_CHANNEL)
        with open("jobmonrc-docker", "r") as f:
            rcdct = json.load(f)
            rcdct['slack_token'] = slack_token
            rcdct['slack_channel'] = slack_channel

        with open("jobmonrc-docker-wsecrets", "w") as f:
            json.dump(rcdct, f)
    else:
        shutil.copy("jobmonrc-docker", "jobmonrc-docker-wsecrets")

    subprocess.call(["docker-compose", "up", "--build", "-d"])
    os.remove("jobmonrc-docker-wsecrets")
