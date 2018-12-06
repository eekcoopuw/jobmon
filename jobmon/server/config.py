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

        # then override with ENV variables
        if "DB_HOST" in os.environ:
            DEFAULT_SERVER_CONFIG["db_host"] = os.environ["DB_HOST"]
        if "DB_PORT" in os.environ:
            DEFAULT_SERVER_CONFIG["db_port"] = os.environ["DB_PORT"]
        if "DB_USER" in os.environ:
            DEFAULT_SERVER_CONFIG["db_user"] = os.environ["DB_USER"]
        if "DB_PASS" in os.environ:
            DEFAULT_SERVER_CONFIG["db_pass"] = os.environ["DB_PASS"]
        if "SLACK_TOKEN" in os.environ:
            DEFAULT_SERVER_CONFIG["slack_token"] = os.environ["SLACK_TOKEN"]
        if "WF_SLACK_CHANNEL" in os.environ:
            DEFAULT_SERVER_CONFIG["wf_slack_channel"] = (
                os.environ["WF_SLACK_CHANNEL"])
        if "NODE_SLACK_CHANNEL" in os.environ:
            DEFAULT_SERVER_CONFIG["node_slack_channel"] = (
                os.environ["NODE_SLACK_CHANNEL"])

        # and finally override using CLI args (if passed)
        # TBD

        return cls(**DEFAULT_SERVER_CONFIG)

    def __init__(self, db_host, db_port, db_user, db_pass, slack_token,
                 wf_slack_channel, node_slack_channel):

        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.conn_str = "mysql://{user}:{pw}@{host}:{port}/docker".format(
            user=db_user, pw=db_pass, host=db_host, port=db_port)

        self.slack_token = slack_token
        self.wf_slack_channel = wf_slack_channel
        self.node_slack_channel = node_slack_channel
