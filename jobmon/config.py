import json
import os

from jobmon.connection_config import ConnectionConfig


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
        'conn_str': 'sqlite://',
        'host': 'localhost',
        'jsm_pub_port': 3456,
        'jsm_rep_port': 3457,
        'jqs_port': 3458,
        'verbose': False}

    def __init__(self, conn_str, host, jsm_rep_port, jsm_pub_port, jqs_port,
                 verbose):

        self.conn_str = conn_str
        self.verbose = False

        self._host = host
        self._jsm_pub_port = jsm_pub_port
        self._jsm_rep_port = jsm_rep_port
        self._jqs_port = jqs_port

        self.jm_rep_conn = ConnectionConfig(
            host=host,
            port=str(jsm_rep_port))
        self.jm_pub_conn = ConnectionConfig(
            host=host,
            port=str(jsm_pub_port))
        self.jqs_rep_conn = ConnectionConfig(
            host=host,
            port=str(jqs_port))

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value
        self.jm_rep_conn.host = self._host
        self.jm_pub_conn.host = self._host
        self.jm_jqs_conn.host = self._host

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

    @staticmethod
    def get_file_opts(filename):
        rcfile = os.path.abspath(os.path.expanduser(filename))
        with open(rcfile) as json_file:
            try:
                config = json.load(json_file)
            except json.JSONDecodeError:
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
config = GlobalConfig.from_file("~/.jobmonrc")
