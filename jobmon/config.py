import argparse
import json
import os

from jobmon.connection_config import ConnectionConfig


class InvalidConfig(Exception):
    pass


class GlobalConfig(object):

    def __init__(self, conn_str, host, jsm_rep_port, jsm_pub_port, jqs_port):

        self.conn_str = conn_str

        self.jm_rep_conn = ConnectionConfig(
            host=host,
            port=str(jsm_rep_port))
        self.jm_pub_conn = ConnectionConfig(
            host=host,
            port=str(jsm_pub_port))
        self.jqs_rep_conn = ConnectionConfig(
            host=host,
            port=str(jqs_port))

    @classmethod
    def from_file(cls, filename="~/.jobmonrc"):
        rcfile = os.path.abspath(os.path.expanduser(filename))
        with open(rcfile) as json_file:
            try:
                config = json.load(json_file)
            except json.JSONDecodeError:
                raise InvalidConfig("Configuration error. {} is not "
                                    "valid JSON".format(rcfile))
        gc_keys = ['conn_str', 'host', 'jsm_rep_port', 'jsm_pub_port',
                   'jqs_port']
        return cls(**{k: v for k, v in config.items() if k in gc_keys})

    @classmethod
    def from_parsed_args(cls, args_to_parse=None):
        parser = argparse.ArgumentParser(description='Configure Jobmon')
        parser.add_argument("--conn_str", required=True)
        parser.add_argument("--host", required=True)
        parser.add_argument("--jsm_rep_port", required=True)
        parser.add_argument("--jsm_pub_port", required=True)
        parser.add_argument("--jqs_port", required=True)
        args, _ = parser.parse_known_args(args_to_parse)
        return cls(args.conn_str, args.host, args.jsm_rep_port,
                   args.jsm_pub_port, args.jqs_port)

    @classmethod
    def from_defaults(cls):
        return cls(conn_str="sqlite://", host="localhost", jsm_rep_port=3456,
                   jsm_pub_port=3457, jqs_port=3458)


try:
    config = GlobalConfig.from_file()
except FileNotFoundError:
    config = GlobalConfig.from_defaults()
