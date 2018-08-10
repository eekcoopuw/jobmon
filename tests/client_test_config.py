import logging
import json
import os
import socket

import jobmon
from jobmon.client.connection_config import ConnectionConfig


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
        "jobmon_version": str(jobmon.__version__),
        "host": socket.gethostname(),
        "jsm_port": 5056,
        "jqs_port": 5058,
        "jobmon_command": derive_jobmon_command_from_env()
    }

    def __init__(self, jobmon_version, host, jsm_port, jqs_port,
                 jobmon_command):

        self._host = host
        self._jsm_port = jsm_port
        self._jqs_port = jqs_port

        self.jsm_conn = ConnectionConfig(
            host=host,
            port=str(jsm_port))
        self.jqs_conn = ConnectionConfig(
            host=host,
            port=str(jqs_port))

        self.jobmon_command = jobmon_command

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value
        self.jsm_conn.host = self._host
        self.jqs_conn.host = self._host

    @property
    def jsm_port(self):
        return self._jsm_port

    @jsm_port.setter
    def jsm_port(self, value):
        self._jsm_port = value
        self.jsm_conn.port = self._jsm_port

    @property
    def jqs_port(self):
        return self._jqs_port

    @jqs_port.setter
    def jqs_port(self, value):
        self._jqs_port = value
        self.jqs_conn.port = self._jqs_port

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

