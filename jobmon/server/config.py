import logging
import os


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

        # Prececdence is CLI > ENV vars > config file

        # then load from default config module
        from jobmon.default_config import DEFAULT_SERVER_CONFIG

        config_opts = DEFAULT_SERVER_CONFIG.copy()

        # then override with ENV variables
        if "DB_HOST" in os.environ:
            config_opts["db_host"] = os.environ["DB_HOST"]
        if "DB_PORT" in os.environ:
            config_opts["db_port"] = os.environ["DB_PORT"]
        if "DB_USER" in os.environ:
            config_opts["db_user"] = os.environ["DB_USER"]
        if "DB_PASS" in os.environ:
            config_opts["db_pass"] = os.environ["DB_PASS"]
        if "DB_NAME" in os.environ:
            config_opts["db_name"] = os.environ["DB_NAME"]
        if "SLACK_TOKEN" in os.environ:
            config_opts["slack_token"] = os.environ["SLACK_TOKEN"]
        if "WF_SLACK_CHANNEL" in os.environ:
            config_opts["wf_slack_channel"] = (
                os.environ["WF_SLACK_CHANNEL"])
        if "NODE_SLACK_CHANNEL" in os.environ:
            config_opts["node_slack_channel"] = (
                os.environ["NODE_SLACK_CHANNEL"])

        # and finally override using CLI args (if passed)
        # TBD

        return cls(**config_opts)

    def __init__(self, db_host, db_port, db_user, db_pass, slack_token,
                 wf_slack_channel, node_slack_channel, db_name='docker'):

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
