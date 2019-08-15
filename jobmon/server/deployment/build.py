import os
from shutil import copyfile

from jobmon.server.deployment import util
from jobmon.server.deployment.util import Conf


class BuildContainer:
    def __init__(self, docker_file_dir, jobmon_root):
        self.envs = dict()
        self.docker_file_dir = docker_file_dir
        # Have to build under the jobmon root dir to install jobmon
        self.jobmon_dir = jobmon_root
        self.tag = Conf().get_docker_tag()

    def _copy_docker_compose_file(self):
        copyfile(self.docker_file_dir + "/" + Conf().get_docker_compose_template(), self.jobmon_dir + "/docker-compose.yml")
        copyfile(self.docker_file_dir + "/Dockerfile", self.jobmon_dir + "/Dockerfile")

    def _dump_env(self):
        filename = self.jobmon_dir + "/.env"
        f = open(filename, "w")
        for k in self.envs.keys():
            f.write(k + "=" + self.envs[k] + "\n")
        f.close()

    def _run_docker_compose(self):
        os.system("cd {} && docker-compose up --build -d".format(self.jobmon_dir))

    def _set_connection_env(self):
        self.envs["EXTERNAL_SERVICE_PORT"] = Conf().get_external_service_port()
        self.envs["EXTERNAL_DB_PORT"] = Conf().get_external_db_port()
        self.envs["INTERNAL_DB_HOST"] = Conf().get_internal_db_host()
        self.envs["INTERNAL_DB_PORT"] = Conf().get_internal_db_port()
        self.envs["JOBMON_VERSION"] = "".join(Conf().get_jobmon_version().split('.'))
        self.envs["SLACK_TOKEN"] = Conf().get_slack_token()
        self.envs["WF_SLACK_CHANNEL"] = Conf().get_wf_slack_channel()
        self.envs["NODE_SLACK_CHANNEL"] = Conf().get_node_slack_channel()
        if Conf().is_existed_db():
            self.envs["JOBMON_PASS_SERVICE_USER"] = Conf().get_jobmon_service_user_pwd()

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

    def build(self):
        self._copy_docker_compose_file()
        self._set_connection_env()
        if not Conf().is_existed_db():
            self._set_mysql_user_passwords()
        self._dump_env()
        self._run_docker_compose()


def main():
    BuildContainer().build()


if __name__ == "__main__":
    main()
