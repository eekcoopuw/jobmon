import logging
import json
import os

import jobmon


logger = logging.getLogger(__file__)

try:
    from json import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


class InvalidConfig(Exception):
    pass


class ServerConfig(object):
    """
    This is intended to be a singleton and should only accessed via the_server_config.

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
        "conn_str": ("mysql://docker:docker@"
                     "jobmon-p01.ihme.washington.edu:3314/docker"),
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

    @property
    def conn_str(self):
        return self._conn_str

    @conn_str.setter
    def conn_str(self, value):
        self._conn_str = value

    def apply_opts_dct(self, opts_dct):
        for opt, opt_val in opts_dct.items():
            if opt == 'conn_str':
                os.environ[opt] = opt_val
            opts_dct[opt] = setattr(self, opt, opt_val)
        return self

    @classmethod
    def from_file(cls, filename):
        opts_dict = cls.get_file_opts(filename)
        opts_dict = cls.apply_defaults(opts_dict)
        return cls(**opts_dict)

    @classmethod
    def from_defaults(cls):
        return cls(**cls.default_opts)

    @staticmethod
    def get_file_opts(filename):
        rcfile = os.path.abspath(os.path.expanduser(filename))
        with open(rcfile) as json_file:
            try:
                config = json.load(json_file)
            except JSONDecodeError:
                raise InvalidConfig("Configuration error. {} is not "
                                    "valid JSON".format(rcfile))
        opts_dict = {k: v for k, v in config.items()
                     if k in ServerConfig.default_opts}
        return opts_dict

    @staticmethod
    def apply_defaults(opts_dict):
        gc_opts = {}
        for opt in ServerConfig.default_opts:
            if opt in opts_dict:
                gc_opts[opt] = opts_dict[opt]
                val = opts_dict[opt]
            else:
                gc_opts[opt] = ServerConfig.default_opts[opt]
                val = ServerConfig.default_opts[opt]
            if opt == 'conn_str':
                os.environ[opt] = val
        return gc_opts

