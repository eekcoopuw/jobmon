import configparser


class SetupCfg:
    __instance = None

    @staticmethod
    def _get_instance():
        if SetupCfg.__instance is None:
            SetupCfg.__instance = configparser.ConfigParser()
            SetupCfg.__instance.read('../setup.cfg')

    def __init__(self):
        SetupCfg._get_instance()

    def is_test_mode(self):
        return SetupCfg.__instance["basic values"]["test_mode"] == "True"

    def get_jobmon_version(self):
        if self.is_test_mode():
            return SetupCfg.__instance["basic values"]["jobmon_version"]
        else:
            from ._version import get_versions
            v = get_versions()['version']
            del get_versions
            return v

    def is_existed_db(self):
        if SetupCfg.__instance["basic values"]["existing_db"] == "True":
            return True
        else:
            return False

    def get_internal_db_host(self):
        if self.is_existed_db():
            return SetupCfg.__instance["existing db"]["internal_db_host"]
        else:
            return SetupCfg.__instance["new db"]["internal_db_host"]

    def get_internal_db_port(self):
        if self.is_existed_db():
            return str(SetupCfg.__instance["existing db"]["internal_db_port"])
        else:
            return str(SetupCfg.__instance["new db"]["internal_db_port"])

    def get_external_db_port(self):
        if self.is_existed_db():
            return str(SetupCfg.__instance["existing db"]["external_db_port"])
        else:
            return str(SetupCfg.__instance["new db"]["external_db_port"])

    def get_external_db_host(self):
        return SetupCfg.__instance["basic values"]["jobmon_server_sqdn"]

    def get_external_service_host(self):
        if self.is_existed_db():
            return str(SetupCfg.__instance["existing db"]["external_db_port"])
        else:
            return str(SetupCfg.__instance["new db"]["external_db_port"])

    def get_external_service_port(self):
        if self.is_existed_db():
            return str(SetupCfg.__instance["existing db"]["jobmon_service_port"])
        else:
            return str(SetupCfg.__instance["new db"]["jobmon_service_port"])

    def get_slack_token(self):
        return SetupCfg.__instance["basic values"]["slack_token"]

    def get_wf_slack_channel(self):
        return SetupCfg.__instance["basic values"]["wf_slack_channel"]

    def get_node_slack_channel(self):
        return SetupCfg.__instance["basic values"]["node_slack_channel"]

    def get_slack_api_url(self):
        return SetupCfg.__instance["basic values"]["slack_api_url"]

    def get_docker_compose_template(self):
        if self.is_existed_db():
            return "docker-compose.yml.existingdb"
        else:
            return "docker-compose.yml.newdb"

    def get_jobmon_service_user_pwd(self):
        return SetupCfg.__instance["existing db"]["jobmon_pass_service_user"]

    def get_docker_tag(self):
        return f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{self.get_jobmon_version()}"

    def get_tag_prefix(self):
        return SetupCfg.__instance["versioneer"]["tag_prefix"]
