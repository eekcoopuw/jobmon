import logging
import socket
from logging.handlers import SysLogHandler
import getpass

from jobmon import config


class SwarmLogging():
    # Constants
    CRITICAL: int = logging.CRITICAL
    ERROR: int = logging.ERROR
    WARNING: int = logging.WARNING
    INFO: int = logging.INFO
    DEBUG: int = logging.DEBUG
    NOTSET: int = logging.NOTSET

    _format: str = ' %(asctime)s [%(name)-12s] %(module)s %(levelname)-8s %(threadName)s: %(message)s'
    _logLevel: int = INFO
    _syslogAttached: bool = config.use_rsyslog

    @staticmethod
    def attach_log_handler(tag: str):
        logger = logging.getLogger()
        logger.setLevel(SwarmLogging._logLevel)
        username = getpass.getuser()
        formatter = logging.Formatter(tag + ": " + username + SwarmLogging._format)
        hs = logging.StreamHandler()
        hs.setFormatter(formatter)
        logger.addHandler(hs)
        if SwarmLogging._syslogAttached:
            p = socket.SOCK_DGRAM
            if config.rsyslog_protocol == "TCP":
                p = socket.SOCK_STREAM
            hr = SysLogHandler(
                address=(config.rsyslog_host, config.rsyslog_port),
                socktype=p)
            hr.setFormatter(formatter)
            logger.addHandler(hr)

    @staticmethod
    def getLogger(name: str = __file__) -> logging.Logger:
        return logging.getLogger(name)
