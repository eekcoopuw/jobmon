#!/usr/bin/env python

import getpass
import json
import os
import random
import socket
import string
import subprocess
from datetime import datetime


INTERNAL_DB_HOST = "db"
INTERNAL_DB_PORT = 3306
EXTERNAL_DB_HOST = "jobmon-p01.ihme.washington.edu"
EXTERNAL_DB_PORT = 3830

EXTERNAL_SERVICE_HOST = "jobmon-p01.ihme.washington.edu"
EXTERNAL_SERVICE_PORT = 8456

DEFAULT_WF_SLACK_CHANNEL = 'jobmon-alerts'
DEFAULT_NODE_SLACK_CHANNEL = 'suspicious_nodes'


def gen_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))


def git_current_commit():
    return subprocess.check_output(
        'git rev-parse HEAD', shell=True, encoding='utf-8').strip()


def git_tags():
    """Finds any tags pointing at the current commit"""
    return subprocess.check_output(
        'git --no-pager tag --points-at HEAD',
        shell=True,
        encoding='utf-8').split()


def find_release(tags):
    """Assumes that the release # can be found in a git tag of the form
    release-#... For example, if the current commit has a tag 'release-1.2.3,'
    we asusme that this deployment corresponds to jobmon version 1.2.3 and the
    function returns '1.2.3'. If no tag matching that form is found, returns
    'no-release'"""
    candidate_tags = [t for t in tags if 'release-' in t]
    if len(candidate_tags) == 1:
        return candidate_tags[0].replace('release-', '')
    elif len(candidate_tags) > 1:
        raise RuntimeError("Multiple release tags found. {}. Cannot deploy "
                           "without a definitive release "
                           "number.".format(candidate_tags))
    else:
        return None


class JobmonDeployment(object):

    def __init__(self, slack_token=None, wf_slack_channel=None,
                 node_slack_channel=None):
        self.info_dir = os.path.expanduser("~/jobmon_deployments")

        self.slack_token = slack_token
        self.wf_slack_channel = wf_slack_channel
        self.node_slack_channel = node_slack_channel

        self.git_commit = git_current_commit()
        self.git_tags = git_tags()
        self.jobmon_version = find_release(self.git_tags)
        if self.jobmon_version is None:
            self.jobmon_version = self.git_commit
        self.deploy_date = datetime.now().strftime("%m%d%Y_%H%M%S")
        self.deploy_user = getpass.getuser()
        self.db_accounts = {}

        # In production, only the svcscicompci user should be allowed to
        # deploy
        if ((socket.gethostname() == "jobmon-p01") and
                (self.deploy_user != 'svcscicompci')):
            raise ValueError("Deployment can only be run by the "
                             "'svcscicompci' service user")

    @property
    def info_file(self):
        versioned_dir = "{d}/{jv}/".format(d=self.info_dir,
                                           jv=self.jobmon_version)
        os.makedirs(versioned_dir, exist_ok=True)
        return "{d}/{date}.info".format(d=versioned_dir, date=self.deploy_date)

    def _create_info_file(self):
        """Write all simple attributes (strings/numbers/single-level lists
        of strings/numbers) of this deployment to a file"""
        writable_types = [str, int, float]
        info = {}
        for attr in dir(self):
            if attr.startswith("__"):
                continue
            val = getattr(self, attr)
            if val in writable_types:
                info[attr] = val
            elif hasattr(val, '__iter__'):
                if all([type(item) in writable_types for item in val]):
                    info[attr] = val
        info['db_accounts'] = self.db_accounts
        with open(self.info_file, "w") as f:
            json.dump(info, f)
        return info

    def _set_connection_env(self):
        os.environ["EXTERNAL_SERVICE_PORT"] = str(EXTERNAL_SERVICE_PORT)
        os.environ["EXTERNAL_DB_PORT"] = str(EXTERNAL_DB_PORT)
        os.environ["INTERNAL_DB_HOST"] = INTERNAL_DB_HOST
        os.environ["INTERNAL_DB_PORT"] = str(INTERNAL_DB_PORT)

    def _run_docker(self):
        subprocess.call(["docker-compose", "up", "--build", "-d"])

    def _set_mysql_user_passwords(self):
        users = ['root', 'table_creator', 'service_user', 'read_only']
        for user in users:
            env_password = os.environ.get('JOBMON_PASS_' + user.upper(),
                                          None)
            if env_password is not None:
                password = env_password
            elif user == "read_only":
                password = "docker"
                os.environ['JOBMON_PASS_' + user.upper()] = password
            else:
                password = self._set_mysql_user_password_env(user)
            self.db_accounts[user] = password

    def _set_mysql_user_password_env(self, user):
        password = gen_password()
        os.environ['JOBMON_PASS_' + user.upper()] = password
        return password

    def run(self):
        self._set_mysql_user_passwords()
        self._set_connection_env()
        self._create_info_file()
        self._run_docker()


def main():

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

        deployment = JobmonDeployment(slack_token=slack_token,
                                      wf_slack_channel=wf_slack_channel,
                                      node_slack_channel=node_slack_channel)
        deployment.run()
    else:
        deployment = JobmonDeployment()
        deployment.run()


if __name__ == "__main__":
    main()
