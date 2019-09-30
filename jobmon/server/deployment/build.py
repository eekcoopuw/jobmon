import os
from shutil import copyfile

from jobmon.server.deployment import git_functions
from jobmon.server.deployment.deployment_config import DeploymentConfig


class BuildContainer:
    def __init__(self, docker_file_dir: str, jobmon_root: str,
                 config: DeploymentConfig = DeploymentConfig.from_defaults()):
        self.envs: dict = dict()
        self.docker_file_dir = docker_file_dir
        # Have to build under the jobmon root dir to install jobmon
        self.jobmon_dir = jobmon_root
        self.config = config

    def _copy_docker_compose_file(self):
        if self.config.existing_db:
            if self.config.same_host:
                # if on the same host connect containers to existing network
                compose_file = "docker-compose.yml.existingdb_same_host"
            else:
                # otherwise connect externally to the containers
                compose_file = "docker-compose.yml.existingdb_diff_host"
        else:
            compose_file = "docker-compose.yml.newdb"

        copyfile(self.docker_file_dir + "/" + compose_file,
                 self.jobmon_dir + "/docker-compose.yml")
        copyfile(self.docker_file_dir + "/Dockerfile",
                 self.jobmon_dir + "/Dockerfile")

    def _dump_env(self):
        filename = self.jobmon_dir + "/.env"
        f = open(filename, "w")
        for k in self.envs.keys():
            f.write(k + "=" + self.envs[k] + "\n")
        f.close()

    def _run_docker_compose(self):
        os.system("cd {} && docker-compose up --build -d"
                  .format(self.jobmon_dir))

    def _set_connection_env(self):
        self.envs["EXTERNAL_SERVICE_PORT"] = self.config.jobmon_service_port
        self.envs["EXTERNAL_DB_HOST"] = self.config.external_db_host
        self.envs["EXTERNAL_DB_PORT"] = self.config.external_db_port
        self.envs["INTERNAL_DB_HOST"] = self.config.internal_db_host
        self.envs["INTERNAL_DB_PORT"] = self.config.internal_db_port
        self.envs["JOBMON_VERSION"] = "".join(
            self.config.jobmon_version.split('.'))
        self.envs["JOBMON_TAG"] = self.config.jobmon_version
        self.envs["SLACK_TOKEN"] = self.config.slack_token
        self.envs["WF_SLACK_CHANNEL"] = self.config.wf_slack_channel
        self.envs["NODE_SLACK_CHANNEL"] = self.config.node_slack_channel
        self.envs["MONITOR_PORT"] = self.config.monitor_port
        self.envs["COMPOSE_PROJECT_NAME"] = self.config.compose_project_name
        if self.config.existing_db:
            self.envs["JOBMON_PASS_SERVICE_USER"] = (
                self.config.jobmon_service_user_pwd)
            if self.config.same_host:
                self.envs["EXISTING_NETWORK"] = self.config.existing_network

    def _set_mysql_user_passwords(self):
        users = ['root', 'table_creator', 'service_user', 'read_only']
        for user in users:
            if 'JOBMON_PASS_' + user.upper() in self.envs:
                password = self.envs['JOBMON_PASS_' + user.upper()]
            elif user == "read_only":
                password = "docker"
            else:
                password = git_functions.gen_password()
            self.envs['JOBMON_PASS_' + user.upper()] = password

    def build(self):
        self._copy_docker_compose_file()
        self._set_connection_env()
        if not self.config.existing_db:
            self._set_mysql_user_passwords()
        self._dump_env()
        self._run_docker_compose()


def main():
    docker_file_dir = os.path.dirname(os.path.abspath(__file__)) + "/container"
    # Have to build under the jobmon root dir to install jobmon
    jobmon_root = os.path.dirname(os.path.abspath(__file__))[
        :0 - len("/jobmon/server/deployment")]
    BuildContainer(docker_file_dir, jobmon_root).build()


if __name__ == "__main__":
    main()
