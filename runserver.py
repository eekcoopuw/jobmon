#!/usr/bin/env python

#import getpass
import json
import os
import shutil
import subprocess


DEFAULT_WF_SLACK_CHANNEL = 'jobmon-alerts'
DEFAULT_NODE_SLACK_CHANNEL = 'suspicious_nodes'


if __name__ == "__main__":

    slack_token = input("Slack bot token: ") or None
    if slack_token:
        wf_slack_channel = (
            input("Slack notification channel for reporting lost workflow "
                  "runs ({}): ".format(DEFAULT_WF_SLACK_CHANNEL)) or
            DEFAULT_WF_SLACK_CHANNEL)
        node_slack_channel = (
            input("Slack notification channel for reporting failing nodes "
                  "({}): ".format(DEFAULT_NODE_SLACK_CHANNEL)) or
            DEFAULT_NODE_SLACK_CHANNEL)


        with open("jobmonrc-docker", "r") as f:
            rcdct = json.load(f)
            rcdct['slack_token'] = slack_token
            rcdct['default_wf_slack_channel'] = wf_slack_channel
            rcdct['default_node_slack_channel'] = node_slack_channel

        with open("jobmonrc-docker-wsecrets", "w") as f:
            json.dump(rcdct, f)

    else:
        shutil.copy("jobmonrc-docker", "jobmonrc-docker-wsecrets")

    subprocess.call(["docker-compose", "up", "--build", "-d"])
    os.remove("jobmonrc-docker-wsecrets")
