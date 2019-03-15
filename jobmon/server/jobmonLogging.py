import logging
import socket

from logging.handlers import SysLogHandler


class jobmonLogging():
    # Constants
    CRITICAL: int = logging.CRITICAL
    ERROR: int = logging.ERROR
    WARNING: int = logging.WARNING
    INFO: int = logging.INFO
    DEBUG: int = logging.DEBUG


    _logger: logging.Logger = None
    _handler: logging.Handler = None
    _format: str = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s'
    _logLevel: int = INFO

    def __init__(self):
        # Add a standard format when the logger is called the first time
        if jobmonLogging._logger is None:
            jobmonLogging._logger = logging.getLogger("jobmonServer")
            # This should output the logs when using docker logs
            jobmonLogging._handler = logging.StreamHandler()
            formatter = logging.Formatter(jobmonLogging._format)
            jobmonLogging._handler.setFormatter(formatter)
            jobmonLogging._logger.setLevel(jobmonLogging._logLevel)
            jobmonLogging._logger.addHandler(jobmonLogging._handler)
            jobmonLogging._handler.setLevel(jobmonLogging._logLevel)

    @staticmethod
    def setlogLevel(level: int):
        if jobmonLogging._logLevel is None:
            jobmonLogging()
        jobmonLogging._logLevel = level
        jobmonLogging._handler.setLevel(level)
        jobmonLogging._logger.setLevel(level)

    @staticmethod
    def getlogLevel() -> int:
        return jobmonLogging._logLevel

    @staticmethod
    def getLogger(name: str = __file__) -> logging.Logger:
        if jobmonLogging._logger is None:
            jobmonLogging()
        jobmonLogging._logger.info("----{}----".format(name))
        return jobmonLogging._logger

    @staticmethod
    def attachHandler(h: logging.Handler, l: int = logging.DEBUG):
        jobmonLogging._logger.addHandler(h)
        h.setLevel(l)
        # The logger log lever needs to be set to the lowest of its handlers
        if l < jobmonLogging._logLevel:
            jobmonLogging.setlogLevel(l)

    @staticmethod
    def attachLocalSyslog(log_dir: str = "/dev/log", l: int = logging.DEBUG):
        h = SysLogHandler(address=log_dir)
        jobmonLogging.attachHandler(h, l)

    @staticmethod
    def attachRemoteSyslog(host: str, port: int, socktype=socket.SOCK_DGRAM, l: int = logging.DEBUG):
        h = SysLogHandler(address=(host, port), socktype=socktype)
        jobmonLogging.attachHandler(h, l)


# The following code is for testing purpose only.
def main():

    logger = jobmonLogging.getLogger()
    print(logging.getLevelName(jobmonLogging.getlogLevel()))
    logger.info("info")
    logger.debug("Debug")
    logger.warning("Warning")
    print()

    jobmonLogging.setlogLevel(logging.DEBUG)
    print(logging.getLevelName(jobmonLogging.getlogLevel()))
    logger.info("info")
    logger.debug("Debug")
    logger.warning("Warning")
    print()

    jobmonLogging.setlogLevel(logging.ERROR)
    print(logging.getLevelName(jobmonLogging.getlogLevel()))
    logger.info("info")
    logger.debug("Debug")
    logger.warning("Warning")
    print()


if __name__ == "__main__":
    main()