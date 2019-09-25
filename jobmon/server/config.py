from jobmon.server.jobmonLogging import jobmonLogging as logging
from jobmon import config


logger = logging.getLogger(__file__)


class InvalidConfig(Exception):
    pass


class ServerConfig(object):
    """
    This is intended to be a singleton. If using in any other way, proceed
    with CAUTION.
    """

    @classmethod
    def from_defaults(cls):
        # then load from default config module
        return cls(db_host=config.external_db_host,
                   db_port=config.external_db_port,
                   slack_token=config.slack_token,
                   wf_slack_channel=config.wf_slack_channel,
                   node_slack_channel=config.node_slack_channel,
                   db_pass=config.jobmon_service_user_pwd)

    def __init__(self, db_host, db_port, slack_token, wf_slack_channel,
                 node_slack_channel, db_pass, db_name='docker',
                 db_user="service_user"):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

        self.slack_token = slack_token
        self.wf_slack_channel = wf_slack_channel
        self.node_slack_channel = node_slack_channel

    @property
    def conn_str(self):
        return "mysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=self.db_user, pw=self.db_pass, host=self.db_host,
            port=self.db_port, db=self.db_name)
