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


class GlobalConfig(object):
    """
    This is intended to be a singleton and should only be instantiated inside
    this module. If at all possible, try to make modify configuration by
    updating your ~/.jobmonrc file.

    If you're a jobmon developer, and you want/need to modify global
    configuration from a different module, import the config singleton and only
    use the setters or modifier methods exposed by GlobalConfig (e.g.
    apply_opts_dct).

    For exmample:

        from jobmon.config import config

        config.jqs_port = 12345
        config.apply_opts_dct({'conn_str': 'my://sql:conn@ection'})


    Note that if you modify the conn_str... you'll also likely need to recreate
    the database engine...

        from jobmon.database import recreate_engine
        recreate_engine()



    TODO: Investigate if there's a more 'pythonic' way to handle this
    """

    default_opts = {
        "jobmon_version": str(jobmon.__version),
        "conn_str": ("mysql://docker:docker@"
                     "jobmon-p01.ihme.washington.edu:3313/docker"),
        "slack_token": None,
        "default_wf_slack_channel": None,
        "default_node_slack_channel": None,
    }

    def __init__(self, jobmon_version, conn_str, slack_token,
                 default_wf_slack_channel, default_node_slack_channel):

        self.jobmon_version = jobmon_version
        self._conn_str = conn_str

        self.slack_token = slack_token
        self.default_wf_slack_channel = default_wf_slack_channel
        self.default_node_slack_channel = default_node_slack_channel

    @property
    def conn_str(self):
        return self._conn_str

    @conn_str.setter
    def conn_str(self, value):
        self._conn_str = value

    def apply_opts_dct(self, opts_dct):
        for opt, opt_val in opts_dct.items():
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
                     if k in GlobalConfig.default_opts}
        return opts_dict

    @staticmethod
    def apply_defaults(opts_dict):
        gc_opts = {}
        for opt in GlobalConfig.default_opts:
            if opt in opts_dict:
                gc_opts[opt] = opts_dict[opt]
            else:
                gc_opts[opt] = GlobalConfig.default_opts[opt]
        return gc_opts


# The client config singleton... if you need to update it, modify the object
# directly via the setter or apply_opts_dct methods. Don't create a new one.
if os.getenv("JOBMON_SERVER_CONFIG"):
    CONFIG_FILE = os.getenv("JOBMON_SERVER_CONFIG")
else:
    CONFIG_FILE = "~/.jobmon_server_rc"
if os.path.isfile(os.path.expanduser(CONFIG_FILE)):
    config = GlobalConfig.from_file(CONFIG_FILE)
    logger.warn("Found a local config file {}. Therefore we cannot configure "
                "from defaults and you may be accessing an out-of-date "
                "server/database. Consider deleting your .jobmonrc and "
                "relaunching".format(CONFIG_FILE))
else:
    config = GlobalConfig.from_defaults()

