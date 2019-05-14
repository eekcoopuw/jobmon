#!/usr/bin/env python

import getpass
import json
import os
import random
import socket
import string
import subprocess
import requests
from datetime import datetime

from jobmon.models.attributes import constants


INTERNAL_DB_HOST = "db"
INTERNAL_DB_PORT = 3306
EXTERNAL_DB_HOST = constants.deploy_attribute["SERVER_QDNS"]
EXTERNAL_DB_PORT = constants.deploy_attribute["DB_PORT"]

EXTERNAL_SERVICE_HOST = constants.deploy_attribute["SERVER_QDNS"]
EXTERNAL_SERVICE_PORT = constants.deploy_attribute["SERVICE_PORT"]

DEFAULT_WF_SLACK_CHANNEL = 'jobmon-alerts'
DEFAULT_NODE_SLACK_CHANNEL = 'suspicious_nodes'
SLACK_API_URL = constants.deploy_attribute['SLACK_API_URL']


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


def validate_slack_token(slack_token: str) -> bool:
    """
    Checks whether a given slack token is valid

    :param slack_token: A Slack Bot User OAuth Access Token
    :return: True if the token validates, False otherwise
    """
    resp = requests.post(
        SLACK_API_URL,
        headers={'Authorization': 'Bearer {}'.format(slack_token)})
    if resp.status_code != 200:
        print(f"Response returned a bad status code: {resp.status_code} "
              f"(Expected 200) with content {resp.json()}. "
              f"Retry with token or skip")
        return False
    return resp.json()['error'] != 'invalid_auth'


class JobmonDeployment(object):

    def __init__(self, slack_token=None, wf_slack_channel=None,
                 node_slack_channel=None, push_to_registry=False):
        self.info_dir = os.path.expanduser("~/jobmon_deployments")

        self.slack_token = slack_token
        self.push_to_registry = push_to_registry
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
        if ((socket.gethostname() == constants.deploy_attribute["SERVER_HOSTNAME"]) and
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
        os.environ["JOBMON_VERSION"] = "".join(self.jobmon_version.split('.'))
        if self.slack_token is not None:
            os.environ["SLACK_TOKEN"] = self.slack_token
        if self.wf_slack_channel is not None:
            os.environ["WF_SLACK_CHANNEL"] = self.wf_slack_channel
        if self.node_slack_channel is not None:
            os.environ["NODE_SLACK_CHANNEL"] = self.node_slack_channel

    def _run_docker(self):
        subprocess.call(["docker-compose", "up", "--build", "-d"])

    def _upload_image(self):
        version = os.environ['JOBMON_VERSION']
        tag = f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{version}"
        subprocess.call(["docker", "push", tag])

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
        if self.push_to_registry:
            self._upload_image()


def main():
    print("Signing into the harbor registry you will be prompted for your "
          "credentials:")
    resp = subprocess.call(
        ["docker", "login", "registry-app-p01.ihme.washington.edu"])
    if resp == 0:
        push_to_registry = True
    else:
        push_to_registry = False

    slack_token = input(
        "Slack bot token (leave empty to skip. no quotes please): ") or None
    while slack_token is not None:
        token_is_valid = validate_slack_token(slack_token)
        if token_is_valid:
            break
        else:
            slack_token = input(
                "Slack bot token (leave empty to skip. no quotes please): "
            ) or None

    if slack_token:
        wf_slack_channel = input(
            "Slack notification channel for reporting lost workflow "
            f"runs. Leave blank to use default ({DEFAULT_WF_SLACK_CHANNEL}). "
            "No quotes please") or DEFAULT_WF_SLACK_CHANNEL
        node_slack_channel = input(
            "Slack notification channel for reporting failing nodes "
            f". Leave blank to use default ({DEFAULT_NODE_SLACK_CHANNEL}). "
            "No quotes please") or DEFAULT_NODE_SLACK_CHANNEL

        deployment = JobmonDeployment(slack_token=slack_token,
                                      wf_slack_channel=wf_slack_channel,
                                      node_slack_channel=node_slack_channel,
                                      push_to_registry=push_to_registry)
        deployment.run()
    else:
        deployment = JobmonDeployment(push_to_registry=push_to_registry)
        deployment.run()


if __name__ == "__main__":
    main()
