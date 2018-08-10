import logging
import json
import os

import jobmon
from cluster_utils.ephemerdb import create_ephemerdb


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
        "jobmon_version": str(jobmon.__version__),
        "conn_str": ("mysql://docker:docker@"
                     "jobmon-p01.ihme.washington.edu:3313/docker"),
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
        self.session_edb = None

        self.slack_token = slack_token
        self.default_wf_slack_channel = default_wf_slack_channel
        self.default_node_slack_channel = default_node_slack_channel

        if self.session_edb is None:
            self.assign_ephemera_conn_str()

    def assign_ephemera_conn_str(self):
        edb = create_ephemerdb()
        self.session_edb = edb
        conn_str = edb.start()
        self._conn_str = conn_str

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


