#!/usr/bin/env python

import os

from jobmon.server.deployment import util
from jobmon.server.deployment.util import conf


class JobmonDeployment:
    def __init__(self, tag):
        self.envs=dict()
        self.docker_file_dir = os.path.dirname(os.path.abspath(__file__)) + "/container"
        # Have to build under the jobmon root dir to install jobmon
        self.jobmon_dir = os.path.dirname(os.path.abspath(__file__))[:0-len("/jobmon/server/deployment")]
        self.tag = tag

    def _copy_docker_compse_file(self):
        cmd = "cp {0}/{1} {2}/docker-compose.yml".format(self.docker_file_dir, conf.getDockerComposeTemplate(), self.jobmon_dir)
        os.system(cmd)

    def _dump_env(self):
        filename = self.jobmon_dir + "/.env"
        f = open(filename, "w")
        for k in self.envs.keys():
            f.write(k + "=" + self.envs[k] + "\n")
        f.close()

    def _run_docker_compose(self):
        os.system("cd {} && docker-compose up --build -d".format(self.jobmon_dir))

    def _set_connection_env(self):
        self.envs["EXTERNAL_SERVICE_PORT"] = conf.getExternalServicePort()
        self.envs["EXTERNAL_DB_PORT"] = conf.getExternalDBPort()
        self.envs["INTERNAL_DB_HOST"] = conf.getInternalDBHost()
        self.envs["INTERNAL_DB_PORT"] = conf.getExternalDBPort()
        self.envs["JOBMON_VERSION"] = "".join(conf.getJobmonVersion().split('.'))
        self.envs["SLACK_TOKEN"] = conf.getSlackToken()
        self.envs["WF_SLACK_CHANNEL"] = conf.getWFSlackChannel()
        self.envs["NODE_SLACK_CHANNEL"] = conf.getNodeSlackChannel()
        if conf.isExistedDB():
            self.envs["JOBMON_PASS_SERVICE_USER"] = conf.getJobmonServiceUserPwd()

    def _set_mysql_user_passwords(self):
        users = ['root', 'table_creator', 'service_user', 'read_only']
        for user in users:
            if 'JOBMON_PASS_' + user.upper() in self.envs:
                password = self.envs['JOBMON_PASS_' + user.upper()]
            elif user == "read_only":
                password = "docker"
            else:
                password = util.gen_password()
            self.envs['JOBMON_PASS_' + user.upper()] = password

    def _upload_image(self):
        os.system("docker push {}".format(self.tag))

    def build(self):
        self._copy_docker_compse_file()
        self._set_connection_env()
        if not conf.isExistedDB():
            self._set_mysql_user_passwords()
        self._dump_env()
        self._run_docker_compose()
        if not conf.isTestMode():
            self._upload_image()

def main():
    version = conf.getJobmonVersion()
    tag = f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{version}"
    JobmonDeployment(tag).build()


if __name__ == "__main__":
    main()