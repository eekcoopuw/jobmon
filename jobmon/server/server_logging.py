import logging
import socket
import inspect
from typing import Any
from flask import Flask
from logging.handlers import SysLogHandler

from jobmon import config


class _LogLevelSingleton:
    __instance = None

    def __init__(self, log_level: int = logging.INFO,
                 log_level_flask=logging.WARNING):
        if _LogLevelSingleton.__instance is None:
            _LogLevelSingleton.__instance = self
            self.log_level = log_level
            self.log_level_flask = log_level_flask

    @staticmethod
    def get_instance():
        if _LogLevelSingleton.__instance is None:
            _LogLevelSingleton()
        return _LogLevelSingleton.__instance

    def set_log_level(self, level: int):
        self.log_level_flask = self.log_level_flask

    def set_log_level_flask(self, level: int):
        self.log_level_flask = level

    def get_log_level(self) -> int:
        return self.log_level

    def get_log_level_flask(self) -> int:
        return self.log_level_flask


class jobmonLogging:
    # Constants
    # This is to make the jsm and jqm code easier to read
    CRITICAL: int = logging.CRITICAL
    ERROR: int = logging.ERROR
    WARNING: int = logging.WARNING
    INFO: int = logging.INFO
    DEBUG: int = logging.DEBUG
    NOTSET: int = logging.NOTSET

    TAG: str = "JOBMON_SERVER"

    _format: str = TAG + ': %(asctime)s [%(name)-12s] %(module)s %(levelname)-8s %(threadName)s: %(message)s'
    _syslogAttached: bool = config.use_rsyslog
    # Flask prints too many logs at INFO level, so set it's level separately

    @staticmethod
    def myself():
        """
        This method gets the running package name and the running method name

        :return: the package name and the method name for logging purpose
        """
        return (jobmonLogging.TAG + ": ----" + inspect.stack()[1][1] + "----" +
                inspect.stack()[1][3])

    @staticmethod
    def createFlaskLoggers():
        # Flask logger
        _flask_logger = Flask(__name__).logger
        _flask_logger.setLevel(
            _LogLevelSingleton.get_instance().get_log_level())
        # werkzeug logger
        _werkzeug_logger = logging.getLogger("werkzeug")
        _werkzeug_logger.setLevel(
            _LogLevelSingleton.get_instance().get_log_level())
        hs = logging.StreamHandler()
        formatter = logging.Formatter(jobmonLogging._format)
        hs.setFormatter(formatter)
        # hs.setLevel(LogLevelSingleton.get_instance().get_log_level_flask())
        _flask_logger.addHandler(hs)
        _werkzeug_logger.addHandler(hs)
        if jobmonLogging.isSyslogAttached():
            p = socket.SOCK_DGRAM
            if config.rsyslog_protocol == "TCP":
                p = socket.SOCK_STREAM
            jobmonLogging.attach_syslog_to_logger(
                _flask_logger, config.rsyslog_host, config.rsyslog_port, p,
                _LogLevelSingleton.get_instance().get_log_level())
            jobmonLogging.attach_syslog_to_logger(
                _werkzeug_logger, config.rsyslog_host, config.rsyslog_port, p,
                _LogLevelSingleton.get_instance().get_log_level())

        # use different handles for sqlalchemy
        _sqlalchemy_logger = logging.getLogger('sqlalchemy')
        _sqlalchemy_logger.setLevel(
            _LogLevelSingleton.get_instance().get_log_level_flask())
        hs = logging.StreamHandler()
        formatter = logging.Formatter(jobmonLogging._format)
        hs.setFormatter(formatter)
        _sqlalchemy_logger.addHandler(hs)
        if jobmonLogging.isSyslogAttached():
            p = socket.SOCK_DGRAM
            if config.rsyslog_protocol == "TCP":
                p = socket.SOCK_STREAM
            jobmonLogging.attach_syslog_to_logger(
                _sqlalchemy_logger, config.rsyslog_host, config.rsyslog_port,
                p, _LogLevelSingleton.get_instance().get_log_level_flask())

    @staticmethod
    def createLoggers():
        root_logger = logging.getLogger()
        root_logger.setLevel(_LogLevelSingleton.get_instance().get_log_level())
        # docker log handler
        _handler = logging.StreamHandler()
        formatter = logging.Formatter(jobmonLogging._format)
        _handler.setFormatter(formatter)
        root_logger.addHandler(_handler)
        root_logger.debug(f"Log level set to {jobmonLogging.getLevelName()}")
        # syslog handler
        if jobmonLogging.isSyslogAttached():
            root_logger.info("Attach initial rsyslog")
            p = socket.SOCK_DGRAM
            if config.rsyslog_protocol == "TCP":
                p = socket.SOCK_STREAM
            jobmonLogging.attach_syslog_to_logger(
                root_logger, config.rsyslog_host, config.rsyslog_port, p,
                _LogLevelSingleton.get_instance().get_log_level())
        jobmonLogging.createFlaskLoggers()

    @staticmethod
    def logParameter(name: str, v: Any):
        return (jobmonLogging.TAG + ": [Name: {n}, Type: {t}, Value: {v}]"
                .format(n=name, t=type(v), v=v))

    @staticmethod
    def setFlaskLogLevel(level: int):
        _LogLevelSingleton.get_instance().set_log_level_flask(level)
        _flask_logger = logging.getLogger('sqlalchemy')
        _flask_logger.warning(
            "sqlalchemy log level set to " + jobmonLogging.getFlaskLevelName())
        _flask_logger.setLevel(level)

    @staticmethod
    def setlogLevel(level: int):
        _LogLevelSingleton.get_instance().set_log_level(level)
        root_logger = logging.getLogger()
        root_logger.warning("Log level set to " + jobmonLogging.getLevelName())
        root_logger.setLevel(level)
        # exclue sqlalchemy logger
        jobmonLogging.setFlaskLogLevel(
            _LogLevelSingleton.get_instance().get_log_level_flask())

    @staticmethod
    def getlogLevel() -> int:
        # Get root log level
        return _LogLevelSingleton.get_instance().get_log_level()

    @staticmethod
    def getFlasklogLevel() -> int:
        # Get jobmonServer log level
        return _LogLevelSingleton.get_instance().get_log_level_flask()

    @staticmethod
    def getLevelName() -> str:
        return logging.getLevelName(
            _LogLevelSingleton.get_instance().get_log_level())

    @staticmethod
    def getFlaskLevelName() -> str:
        return logging.getLevelName(
            _LogLevelSingleton.get_instance().get_log_level_flask())

    @staticmethod
    def getLogger(name: str = __file__) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.debug(jobmonLogging.myself())
        return logger

    @staticmethod
    def attachHandler(logger, h: logging.Handler, l: int = logging.NOTSET):
        formatter = logging.Formatter(jobmonLogging._format)
        h.setFormatter(formatter)
        h.setLevel(l)
        logger.addHandler(h)

    @staticmethod
    def attach_syslog_to_logger(logger, host: str, port: int,
                                socktype=socket.SOCK_DGRAM,
                                l: int = logging.NOTSET):
        logger.debug(jobmonLogging.myself())
        logger.debug(jobmonLogging.logParameter("host", host))
        logger.debug(jobmonLogging.logParameter("port", port))
        logger.debug(jobmonLogging.logParameter("socktype", socktype))
        logger.debug(jobmonLogging.logParameter("l", l))
        h = SysLogHandler(address=(host, port), socktype=socktype)
        jobmonLogging.attachHandler(logger, h, l)
        if socktype == socket.SOCK_DGRAM:
            logger.debug(
                "UDP syslog handler {h}:{p} attached".format(h=host, p=port))
        else:
            logger.debug(
                "TCP syslog handler {h}:{p} attached".format(h=host, p=port))
        jobmonLogging._syslogAttached = True

    @staticmethod
    def attachSyslog(host: str, port: int, socktype=socket.SOCK_DGRAM,
                     l: int = logging.DEBUG):
        jobmonLogging.attach_syslog_to_logger(logging.getLogger(), host, port,
                                              socktype, l)

    @staticmethod
    def isSyslogAttached():
        return jobmonLogging._syslogAttached
