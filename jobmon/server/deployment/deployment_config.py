from jobmon import config


class DeploymentConfig(object):

    @classmethod
    def from_defaults(cls):
        cls(jobmon_service_port=config.jobmon_service_port,
            external_db_host=config.external_db_host,
            external_db_port=config.external_db_port,
            internal_db_host=config.internal_db_host,
            internal_db_port=config.internal_db_port,
            jobmon_version=config.jobmon_version,
            slack_token=config.slack_token,
            wf_slack_channel=config.wf_slack_channel,
            node_slack_channel=config.node_slack_channel,
            monitor_port=config.monitor_port,
            jobmon_service_user_pwd=config.jobmon_service_user_pwd,
            existing_network=config.existing_network,
            same_host=config.same_host,
            existing_db=config.existing_db
            )

    def __init__(self, jobmon_service_port, external_db_port,
                 external_db_host, internal_db_port, internal_db_host,
                 jobmon_version, slack_token, wf_slack_channel,
                 node_slack_channel, monitor_port, compose_project_name,
                 jobmon_service_user_pwd, existing_network,
                 same_host, existing_db):
        self.jobmon_service_port = jobmon_service_port
        self.external_db_port = external_db_port
        self.external_db_host = external_db_host
        self.internal_db_port = internal_db_port
        self.internal_db_host = internal_db_host
        self.jobmon_version = jobmon_version
        self.slack_token = slack_token
        self.wf_slack_channel = wf_slack_channel
        self.node_slack_channel = node_slack_channel
        self.monitor_port = monitor_port
        self.compose_project_name = compose_project_name
        self.jobmon_service_user_pwd = jobmon_service_user_pwd
        self.existing_network = existing_network
        self.same_host = same_host
        self.existing_db = existing_db

    @property
    def compose_project_name(self):
        if self.existing_db:
            return f"existing_db_{self.jobmon_version}"
        else:
            return f"new_db_{self.jobmon_version}"

    @property
    def docker_tag(self):
        return ("registry-app-p01.ihme.washington.edu/jobmon/jobmon:" +
                self.jobmon_version)
