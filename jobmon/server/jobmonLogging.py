import logging
import socket
import inspect
from flask.logging import default_handler
from flask import Flask

from logging.handlers import SysLogHandler


class jobmonLogging():
    # Constants
    CRITICAL: int = logging.CRITICAL
    ERROR: int = logging.ERROR
    WARNING: int = logging.WARNING
    INFO: int = logging.INFO
    DEBUG: int = logging.DEBUG
    NOTSET: int = logging.NOTSET

    _logger: logging.Logger = None
    _handler: logging.Handler = None
    _format: str = '%(asctime)s [%(name)-12s] %(module)s %(levelname)-8s: %(message)s'
    _logLevel: int = INFO
    _syslogAttached: bool = False
    # The list holds specific loggers we want to monitor
    # So far we have flask and sqlalchemy
    _loggerArray: list = []

    myself = lambda: "----" + inspect.stack()[1][1] + "----" + inspect.stack()[1][3]

    @staticmethod
    def _createSpecialLoggers():
        # Create formatter
        formatter: str = '%(asctime)s %(remote_addr)s requested %(url)s %(levelname) %(module)s: %(message)s'

        # Flask logger
        flask_logger = Flask(__name__).logger
        flask_logger.setLevel(jobmonLogging._logLevel)
        default_handler = flask_logger.handlers[0]
        jobmonLogging._loggerArray.append(flask_logger)
        # sqlalchemy logger
        sqlalchemy_logger = logging.getLogger('sqlalchemy')
        sqlalchemy_logger.setLevel(jobmonLogging._logLevel)
        sqlalchemy_logger.addHandler(default_handler)
        jobmonLogging._loggerArray.append(sqlalchemy_logger)


    def __init__(self):
        # Add a standard format when the logger is called the first time
        if jobmonLogging._logger is None:
            jobmonLogging._logger = logging.getLogger("jobmonServer")
            jobmonLogging._logger.debug(jobmonLogging.myself())
            # This should output the logs when using docker logs
            jobmonLogging._handler = logging.StreamHandler()
            formatter = logging.Formatter(jobmonLogging._format)
            jobmonLogging._handler.setFormatter(formatter)
            jobmonLogging._logger.setLevel(jobmonLogging._logLevel)
            jobmonLogging._logger.addHandler(jobmonLogging._handler)
            jobmonLogging._logger.debug("Stream log haddler attached")
            jobmonLogging._handler.setLevel(jobmonLogging._logLevel)
            jobmonLogging._logger.debug("Log level set to {}".format(jobmonLogging.getLevelName()))
            jobmonLogging._loggerArray.append(jobmonLogging._logger)
            jobmonLogging._createSpecialLoggers()

    @staticmethod
    def logParameter(name: str, v: any):
        return "[Name: {n}, Type: {t}, Value: {v}]".format(n=name, t=type(v), v=v)

    @staticmethod
    def setlogLevel(level: int):
        if jobmonLogging._logger is None:
            jobmonLogging()
        jobmonLogging._logger.debug(jobmonLogging.myself())
        jobmonLogging._logger.debug(jobmonLogging.logParameter("level", level))
        jobmonLogging._logLevel = level
        jobmonLogging._logger.info("----------------Log Lever Set to {}----------------".format(jobmonLogging.getLevelName()))
        for l in jobmonLogging._loggerArray:
            for h in l.handlers:
                h.setLevel(level)
        jobmonLogging._logger.setLevel(level)
        jobmonLogging._logger.debug("Log level set to {}".format(jobmonLogging.getLevelName()))

    @staticmethod
    def getlogLevel() -> int:
        jobmonLogging._logger.debug(jobmonLogging.myself())
        return jobmonLogging._logLevel

    @staticmethod
    def getLevelName() -> str:
        jobmonLogging._logger.debug(jobmonLogging.myself())
        return logging.getLevelName(jobmonLogging._logLevel)

    @staticmethod
    def getLogger(name: str = __file__) -> logging.Logger:
        if jobmonLogging._logger is None:
            jobmonLogging()
        jobmonLogging._logger.debug(jobmonLogging.myself())
        jobmonLogging._logger.debug(jobmonLogging.logParameter("name", name))
        return jobmonLogging._logger

    @staticmethod
    def attachHandler(h: logging.Handler, l: int = logging.DEBUG):
        jobmonLogging._logger.debug(jobmonLogging.myself())
        jobmonLogging._logger.addHandler(h)
        jobmonLogging._logger.debug("New log handler attached")
        h.setLevel(l)
        # The logger log lever needs to be set to the lowest of its handlers
        if l < jobmonLogging._logLevel:
            jobmonLogging.setlogLevel(l)

    @staticmethod
    def attachSyslog(host: str, port: int, socktype=socket.SOCK_DGRAM, l: int = logging.DEBUG):
        jobmonLogging._logger.debug(jobmonLogging.myself())
        jobmonLogging._logger.debug(jobmonLogging.logParameter("host", host))
        jobmonLogging._logger.debug(jobmonLogging.logParameter("port", port))
        jobmonLogging._logger.debug(jobmonLogging.logParameter("socktype", socktype))
        jobmonLogging._logger.debug(jobmonLogging.logParameter("l", l))
        h = SysLogHandler(address=(host, port), socktype=socktype)
        jobmonLogging.attachHandler(h, l)
        if socktype == socket.SOCK_DGRAM:
            jobmonLogging._logger.debug("UDP syslog handler {h}:{p} attached".format(h=host, p=port))
        else:
            jobmonLogging._logger.debug("TCP syslog handler {h}:{p} attached".format(h=host, p=port))
        jobmonLogging._syslogAttached = True

    @staticmethod
    def isSyslogAttached():
        jobmonLogging._logger.debug(jobmonLogging.myself())
        jobmonLogging._logger.debug("Syslog attached: {}".format(jobmonLogging._syslogAttached))
        return jobmonLogging._syslogAttached

    @staticmethod
    def _setRootLoggerLevel(level: int):
        # Based on my testing, this seems to work one way. You can change from INFO to DEBUG, but unable to change back.
        jobmonLogging._logger.info("!!!   You are changing the root logger level to {}   !!!".format(logging.getLevelName(level)))
        root = logging.getLogger()
        root.debug("before change")
        formatter = logging.Formatter(jobmonLogging._format)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        handler.setLevel(level)
        root.addHandler(handler)
        root.setLevel(level)
        root.debug("after change")
