#!/usr/bin/env python

import getpass
import json
import os
import random
import string
import subprocess
from datetime import datetime

import yaml


DEFAULT_WF_SLACK_CHANNEL = 'jobmon-alerts'
DEFAULT_NODE_SLACK_CHANNEL = 'suspicious_nodes'


def gen_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))


def git_current_commit():
    return subprocess.check_output(
        'git rev-parse HEAD', shell=True, encoding='utf-8').strip()


def git_tags():
    return subprocess.check_output(
        'git --no-pager tag --points-at HEAD',
        shell=True,
        encoding='utf-8').split()


def find_release(tags):
    candidate_tags = [t for t in tags if 'release-' in t]
    if len(candidate_tags) == 1:
        return candidate_tags[0].replace('release-', '')
    elif len(candidate_tags) > 1:
        raise RuntimeError("Multiple release tags found. {}. Cannot deploy "
                           "without a definitive release "
                           "number.".format(candidate_tags))
    else:
        return 'no-release'


def parse_docker_compose(dc_file):
    with open(dc_file, "r") as dcf:
        dc_dct = yaml.load(dcf)
    return dc_dct


class JobmonDeployment(object):

    def __init__(self, slack_token=None, wf_slack_channel=None,
                 node_slack_channel=None):
        self.template_rcfile = 'jobmonrc-docker'
        self.initdb_rcfile = 'jobmonrc-docker-initdb'
        self.service_rcfile = 'jobmonrc-docker-wsecrets'
        self.info_dir = os.path.expanduser("~/jobmon_deployments")

        self.slack_token = slack_token
        self.wf_slack_channel = wf_slack_channel
        self.node_slack_channel = node_slack_channel

        self.git_commit = git_current_commit()
        self.git_tags = git_tags()
        self.jobmon_version = find_release(self.git_tags)
        self.deploy_date = datetime.now().strftime("%m%d%Y_%H%M%S")
        self.deploy_user = getpass.getuser()
        self.db_accounts = {}
        # if self.deploy_user != 'svcscicompci':
        #     raise ValueError("Deployment can only be run by the "
        #                      "'svcscicompci' service user")

    @property
    def info_file(self):
        versioned_dir = "{d}/{jv}/".format(d=self.info_dir,
                                           jv=self.jobmon_version)
        os.makedirs(versioned_dir, exist_ok=True)
        return "{d}/{date}.info".format(d=versioned_dir, date=self.deploy_date)

    @property
    def external_db_port(self):
        dc_dct = parse_docker_compose("docker-compose.yml")
        return dc_dct['services']['db']['ports'][0].split(":")[0]

    def _construct_conn_str(self, user):
        return "mysql://{user}:{pw}@db:3306/docker".format(
            user=user,
            pw=self.db_accounts[user]
        )

    def _cleanup(self):
        os.remove(self.service_rcfile)

    def _create_jobmonrc_file(self):
        with open(self.template_rcfile, "r") as f:
            rcdct = json.load(f)
            if self.slack_token:
                rcdct['slack_token'] = self.slack_token
                rcdct['default_wf_slack_channel'] = self.wf_slack_channel
                rcdct['default_node_slack_channel'] = self.node_slack_channel

        rcdct['conn_str'] = self._construct_conn_str('service_user')
        with open(self.service_rcfile, "w") as f:
            json.dump(rcdct, f)
        rcdct['conn_str'] = self._construct_conn_str('table_creator')
        with open(self.initdb_rcfile, "w") as f:
            json.dump(rcdct, f)

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

    def _run_docker(self):
        subprocess.call(["docker-compose", "up", "--build", "-d"])

    def _set_mysql_user_passwords(self):
        users = ['root', 'table_creator', 'service_user', 'read_only']
        for user in users:
            password = self._set_mysql_user_password_var(user)
            self.db_accounts[user] = password

    def _set_mysql_user_password_var(self, user):
        password = gen_password()
        os.environ['JOBMON_PASS_' + user.upper()] = password
        return password

    def run(self):
        self._set_mysql_user_passwords()
        self._create_jobmonrc_file()
        self._create_info_file()
        self._run_docker()
        self._cleanup()


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
