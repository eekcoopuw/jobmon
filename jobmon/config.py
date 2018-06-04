import logging
import json
import os

from jobmon.connection_config import ConnectionConfig


logger = logging.getLogger(__file__)

try:
    from json import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


def derive_jobmon_command_from_env():
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return (
            'singularity run --app jobmon_command {}'
            .format(singularity_img_path).encode())
    return None


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
        "conn_str": ("mysql://docker:docker@"
                     "jobmon-p01.ihme.washington.edu:3312/docker"),
        "jsm_host": "jobmon-p01.ihme.washington.edu",
        "jqs_host": "jobmon-p01.ihme.washington.edu",
        "jsm_rep_port": 5056,
        "jsm_pub_port": 5057,
        "jqs_port": 5058,
        "verbose": False,
        "slack_token": None,
        "default_wf_slack_channel": None,
        "default_node_slack_channel": None,
        "jobmon_command": derive_jobmon_command_from_env()
    }

    def __init__(self, conn_str, jsm_host, jqs_host, jsm_rep_port,
                 jsm_pub_port, jqs_port, verbose, slack_token,
                 default_wf_slack_channel, default_node_slack_channel,
                 jobmon_command):

        self.conn_str = conn_str
        self.verbose = False

        self._jsm_host = jsm_host
        self._jqs_host = jqs_host
        self._jsm_pub_port = jsm_pub_port
        self._jsm_rep_port = jsm_rep_port
        self._jqs_port = jqs_port

        self.jm_rep_conn = ConnectionConfig(
            host=jsm_host,
            port=str(jsm_rep_port))
        self.jm_pub_conn = ConnectionConfig(
            host=jsm_host,
            port=str(jsm_pub_port))
        self.jqs_rep_conn = ConnectionConfig(
            host=jqs_host,
            port=str(jqs_port))

        self.slack_token = slack_token
        self.default_wf_slack_channel = default_wf_slack_channel
        self.default_node_slack_channel = default_node_slack_channel
        self.jobmon_command = jobmon_command

    @property
    def jsm_host(self):
        return self._jsm_host

    @jsm_host.setter
    def jsm_host(self, value):
        self._jsm_host = value
        self.jm_rep_conn.host = self._jsm_host
        self.jm_pub_conn.host = self._jsm_host

    @property
    def jqs_host(self):
        return self._jqs_host

    @jqs_host.setter
    def jqs_host(self, value):
        self._jqs_host = value
        self.jqs_rep_conn.host = self._jqs_host

    @property
    def jsm_pub_port(self):
        return self._jsm_pub_port

    @jsm_pub_port.setter
    def jsm_pub_port(self, value):
        self._jsm_pub_port = value
        self.jm_pub_conn.port = self._jsm_pub_port

    @property
    def jsm_rep_port(self):
        return self._jsm_rep_port

    @jsm_rep_port.setter
    def jsm_rep_port(self, value):
        self._jsm_rep_port = value
        self.jm_rep_conn.port = self._jsm_rep_port

    @property
    def jqs_port(self):
        return self._jqs_port

    @jqs_port.setter
    def jqs_port(self, value):
        self._jqs_port = value
        self.jqs_rep_conn.port = self._jqs_port

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


# The config singleton... if you need to update it, modify the object directly
# via the setter or apply_opts_dct methods. Don't create a new one.
if os.getenv("JOBMON_CONFIG"):
    CONFIG_FILE = os.getenv("JOBMON_CONFIG")
else:
    CONFIG_FILE = "~/.jobmonrc"
if os.path.isfile(os.path.expanduser(CONFIG_FILE)):
    config = GlobalConfig.from_file(CONFIG_FILE)
    logger.warn("Found a local config file {}. Therefore we cannot configure "
                "from defaults and you may be accessing an out-of-date "
                "server/database. Consider deleting your .jobmonrc and "
                "relaunching".format(CONFIG_FILE))
else:
    config = GlobalConfig.from_defaults()
