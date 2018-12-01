import logging
import os

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


class ClientConfig(object):
    """
    This is intended to be a singleton and should only accessed via
    the_client_config.

    If you're a jobmon developer, and you want/need to modify global
    configuration from a different module, import the config singleton and only
    use the setters or modifier methods exposed by ClientConfig (e.g.
    apply_opts_dct).

    For example:

        from jobmon.the_client_config import get_the_client_config

        config = get_the_client_config()
        config.apply_opts_dct({'port': '12345'})
    """

    default_opts = {
        "jobmon_version": str(jobmon.__version__),
        "host": "jobmon-p01.ihme.washington.edu",
        "port": 7256,
        "jobmon_command": derive_jobmon_command_from_env()
    }

    def __init__(self, jobmon_version, host, port, jobmon_command):

        self._host = host
        self._port = port

        self.jm_conn = ConnectionConfig(
            host=host,
            port=str(port))

        self.jobmon_version = jobmon_version
        self.jobmon_command = jobmon_command
