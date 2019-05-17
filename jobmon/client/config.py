import logging
import os

from jobmon.client.connection_config import ConnectionConfig


logger = logging.getLogger(__file__)


class InvalidConfig(Exception):
    pass


class ClientConfig(object):
    """
    This is intended to be a singleton. Any other usage should be done with
    CAUTION.
    """

    @classmethod
    def from_defaults(cls):

        # Prececdence is CLI > ENV vars > config file

        # then load from default config module
        from jobmon.default_config import DEFAULT_CLIENT_CONFIG

        # then override with ENV variables
        if "JOBMON_HOST" in os.environ:
            DEFAULT_CLIENT_CONFIG["host"] = os.environ["JOBMON_HOST"]
        if "JOBMON_PORT" in os.environ:
            DEFAULT_CLIENT_CONFIG["port"] = os.environ["JOBMON_PORT"]
        if "JOBMON_COMMAND" in os.environ:
            DEFAULT_CLIENT_CONFIG["jobmon_command"] = (
                os.environ["JOBMON_COMMAND"])
        if "RECONCILIATION_INTERVAL" in os.environ:
            DEFAULT_CLIENT_CONFIG["reconciliation_interval"] = (
                int(os.environ["RECONCILIATION_INTERVAL"]))
        if "HEARTBEAT_INTERVAL" in os.environ:
            DEFAULT_CLIENT_CONFIG["heartbeat_interval"] = (
                int(os.environ["HEARTBEAT_INTERVAL"]))
        if "REPORT_BY_BUFFER" in os.environ:
            DEFAULT_CLIENT_CONFIG["report_by_buffer"] = (
                float(os.environ["REPORT_BY_BUFFER"]))
        if "LOST_TRACK_TIMEOUT" in os.environ:
            DEFAULT_CLIENT_CONFIG["lost_track_timeout"] = (
                float(os.environ["LOST_TRACK_TIMEOUT"]))
        # and finally override using CLI args (if passed)
        # TBD

        return cls(**DEFAULT_CLIENT_CONFIG)

    def __init__(self, host, port, jobmon_command, reconciliation_interval,
                 heartbeat_interval, report_by_buffer, lost_track_timeout):

        self._host = host
        self._port = port

        self.jm_conn = ConnectionConfig(
            host=host,
            port=str(port))

        self.jobmon_command = jobmon_command
        self.reconciliation_interval = reconciliation_interval
        self.heartbeat_interval = heartbeat_interval
        self.report_by_buffer = report_by_buffer
        self.lost_track_timeout = lost_track_timeout
