import logging
import socket
from logging.handlers import SysLogHandler
import getpass
import sys
import platform

from jobmon import config


class _ClientLoggingFilter(logging.Filter):
    hostname = platform.node()
    username = getpass.getuser()
    tag = "JOBMON_CLIENT"

    def filter(self, record):
        record.hostname = _ClientLoggingFilter.hostname
        record.username = _ClientLoggingFilter.username
        name = record.name
        if "jobmon.client.swarm" in name:
            record.tag = "JOBMON_SWARM"
        elif "jobmon.client.worder_node" in name:
            record.tag = "JOBMON_NODE"
        else:
            record.tag = _ClientLoggingFilter.tag
        return True


class ClientLogging():

    # Constants
    CRITICAL: int = logging.CRITICAL
    ERROR: int = logging.ERROR
    WARNING: int = logging.WARNING
    INFO: int = logging.INFO
    DEBUG: int = logging.DEBUG
    NOTSET: int = logging.NOTSET

    _format: str = '%(tag)s: %(asctime)s %(username)s [%(hostname)s] [%(name)-12s] %(levelname)-8s %(threadName)s: %(message)s'
    _logLevel: int = WARNING
    _syslogLevel: int = INFO
    _syslogAttached: bool = config.use_rsyslog
    _handlerAttached: bool = False

    @staticmethod
    def remove_log_handler(logger):
        # Not used in 1.1.2
        for h in logger.handlers:
            logger.removeHandler(h)

    @staticmethod
    def attach_log_handler():
        if ClientLogging._handlerAttached:
            return
        ClientLogging._handlerAttached = True
        logger = logging.getLogger("jobmon.client")
        formatter = logging.Formatter(ClientLogging._format)
        h = logging.StreamHandler(sys.stdout)
        h.addFilter(_ClientLoggingFilter())
        h.setLevel(ClientLogging._logLevel)
        h.setFormatter(formatter)
        logger.addHandler(h)
        if ClientLogging._syslogAttached:
            p = socket.SOCK_DGRAM
            if config.rsyslog_protocol == "TCP":
                p = socket.SOCK_STREAM
            hr = SysLogHandler(
                address=(config.rsyslog_host, config.rsyslog_port),
                socktype=p)
            hr.addFilter(_ClientLoggingFilter())
            hr.setFormatter(formatter)
            hr.setLevel(ClientLogging._syslogLevel)
            logger.addHandler(hr)

    @staticmethod
    def getLogger(name: str = __file__) -> logging.Logger:
        logger = logging.getLogger(name)
        return logger


