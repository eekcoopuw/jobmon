#!/usr/bin/env python

import subprocess
from jobmon.server.deployment.util import conf


def _run_docker(self):
    subprocess.call(["docker-compose", "up", "--build", "-d"])

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


def _upload_image(tag: str):
    subprocess.call(["docker", "push", tag])


def main():
    version = conf.getJobmonVersion()
    tag = f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{version}"
    if conf.isTestMode():
        print("You are in test mode. No image push has been done.")
    else:
        _upload_image(tag)


if __name__ == "__main__":
    main()