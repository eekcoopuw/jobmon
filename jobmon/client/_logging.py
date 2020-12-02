import logging
import socket
from logging.handlers import SysLogHandler
import getpass
import sys
import platform

from jobmon import __version__
from jobmon.log_config import configure_logger


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
        elif "jobmon.client.execution" in name:
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

    _format: str = '%(tag)s: %(asctime)s %(username)s [%(hostname)s]'  + str(__version__) + \
                   '[%(name)-12s] %(levelname)-8s %(threadName)s: %(message)s'
    _logLevel: int = WARNING
    _syslogLevel: int = INFO
    # _syslogAttached: bool = config.use_rsyslog
    _syslogAttached: bool = False
    _handlerAttached: bool = False

    def remove_log_handler(logger):
        # Not used in 1.1.2
        for h in logger.handlers:
            logger.removeHandler(h)

    @staticmethod
    def attach_log_handler(use_rsyslog=None, rsyslog_host=None, rsyslog_port=None,
                           rsyslog_protocol=None):
        if ClientLogging._handlerAttached:
            return
        ClientLogging._handlerAttached = True
        logger = ClientLogging.getLogger("jobmon.client", use_rsyslog, rsyslog_host,
                                         rsyslog_port, rsyslog_protocol)
        formatter = logging.Formatter(ClientLogging._format)
        h = logging.StreamHandler(sys.stdout)
        h.addFilter(_ClientLoggingFilter())
        h.setLevel(ClientLogging._logLevel)
        h.setFormatter(formatter)
        logger.addHandler(h)

    @staticmethod
    def getLogger(name: str = __file__, use_rsyslog=None, rsyslog_host=None,
                  rsyslog_port=None, rsyslog_protocol=None) -> logging.Logger:
        logger = configure_logger(name, use_rsyslog, rsyslog_host, rsyslog_port,
                                  rsyslog_protocol)
        return logger
