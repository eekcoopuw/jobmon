import logging

import jobmon


logger = logging.getLogger(__file__)


class InvalidConfig(Exception):
    pass


class ServerConfig(object):
    """
    This is intended to be a singleton and should only accessed via
    the_server_config.

    If you're a jobmon developer, and you want/need to modify global
    configuration from a different module, import the config singleton and only
    use the setters or modifier methods exposed by ServerConfig (e.g.
    apply_opts_dct).

    For example:

        from jobmon.the_server_config import get_the_server_config

        config.apply_opts_dct({'conn_str': 'my://sql:conn@ection'})


    Note that if you modify the conn_str... you'll also likely need to recreate
    the database engine...

        from jobmon.server.database import recreate_engine
        recreate_engine()
    """

    default_opts = {
        "jobmon_version": str(jobmon.__version__),
        "conn_str": ("mysql://read_only:docker@"
                     "jobmon-p01.ihme.washington.edu:3317/docker"),
        "slack_token": None,
        "default_wf_slack_channel": None,
        "default_node_slack_channel": None,
        "verbose": False
    }

    def __init__(self, jobmon_version, conn_str, slack_token,
                 default_wf_slack_channel, default_node_slack_channel,
                 verbose):

        self.jobmon_version = jobmon_version
        self._conn_str = conn_str

        self.slack_token = slack_token
        self.default_wf_slack_channel = default_wf_slack_channel
        self.default_node_slack_channel = default_node_slack_channel

        self.verbose = verbose
