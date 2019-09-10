import configparser
import os


class SetupCfg:
    __instance = None

    @staticmethod
    def _get_instance():
        if SetupCfg.__instance is None:
            SetupCfg.__instance = configparser.ConfigParser()
            config_file = os.path.join(os.path.dirname(__file__), 'jobmon.cfg')
            print(config_file)
            SetupCfg.__instance.read(config_file)
        return SetupCfg.__instance

    def __init__(self):
        self.instance = SetupCfg._get_instance()

    def is_test_mode(self) ->bool:
        return self.instance["basic values"]["test_mode"] == "True"

    def get_jobmon_version(self) ->str:
        if self.is_test_mode():
            return self.instance["basic values"]["jobmon_version"]
        else:
            from jobmon import __version__
            return __version__

    def is_existing_db(self) ->bool:
        return self.instance["basic values"]["existing_db"] == "True"

    def get_internal_db_host(self) ->bool:
        if self.is_existing_db():
            return self.instance["existing db"]["internal_db_host"]
        else:
            return self.instance["new db"]["internal_db_host"]

    def get_internal_db_port(self) ->str:
        if self.is_existing_db():
            return self.instance["existing db"]["internal_db_port"]
        else:
            return self.instance["new db"]["internal_db_port"]

    def get_external_db_port(self) ->str:
        if self.is_existing_db():
            return str(self.instance["existing db"]["external_db_port"])
        else:
            return str(self.instance["new db"]["external_db_port"])

    def get_external_service_host(self) ->str:
        return self.instance["basic values"]["jobmon_server_sqdn"]

    def get_external_db_host(self) ->str:
        if self.is_existing_db():
            return str(self.instance["existing db"]["external_db_host"])
        else:
            return str(self.instance["new db"]["external_db_host"])

    def get_external_service_port(self) ->str:
        return str(self.instance["basic values"]["jobmon_service_port"])

    def get_monitor_port(self):
        return str(self.instance["basic values"]["jobmon_monitor_port"])

    def get_slack_token(self) ->str:
        return self.instance["basic values"]["slack_token"]

    def get_wf_slack_channel(self) ->str:
        return self.instance["basic values"]["wf_slack_channel"]

    def get_node_slack_channel(self) ->str:
        return self.instance["basic values"]["node_slack_channel"]

    def get_slack_api_url(self) ->str:
        return self.instance["basic values"]["slack_api_url"]

    def get_docker_compose_template(self) ->str:
        if self.is_existing_db():
            return "docker-compose.yml.existingdb"
        else:
            return "docker-compose.yml.newdb"

    def get_jobmon_service_user_pwd(self) ->str:
        return self.instance["existing db"]["jobmon_pass_service_user"]

    def get_docker_tag(self) ->str:
        return f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{self.get_jobmon_version()}"

    def get_tag_prefix(self) ->str:
        return self.instance["basic values"]["tag_prefix"]

    def get_reconciliation_interval(self) ->str:
        return self.instance["basic values"]["reconciliation_interval"]

    def get_heartbeat_interval(self) ->str:
        return self.instance["basic values"]["heartbeat_interval"]

    def get_report_by_buffer(self) ->str:
        return self.instance["basic values"]["report_by_buffer"]
