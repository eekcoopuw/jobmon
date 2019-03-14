import logging
import sys


# this is a pointer to the module object instance itself.
this = sys.modules[__name__]

this.stream_handler = None
this.format = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s'
this.level = logging.INFO


def init_handler():
    this.stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(this.format)
    this.stream_handler.setFormatter(formatter)


def getLogLevel() -> str:
    return logging.getLevelName(this.level)


def setLogLevel(l: int):
    this.level = l


def getLogger(name: str):
    # Return the logger. If the handler is not created, create it and attach to the logger
    logger = logging.getLogger(name)
    if this.stream_handler is None:
        init_handler()
    logger.addHandler(this.stream_handler)
    logger.setLevel(this.level)
    return logger

"""
class jobmonLogging():
    _logger = None
    _handler = None
    _format = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s'
    _logLevel = logging.DEBUG

    def __init__(self):
        # Add a standard format when the logger is called the first time
        if jobmonLogging._logger is None:
            jobmonLogging._logger = logging.getLogger("jobmonServer")
            #This should output the logs when using docker logs
            jobmonLogging._handler = logging.StreamHandler()
            formatter = logging.Formatter(jobmonLogging._format)
            jobmonLogging._handler.setFormatter(formatter)
            jobmonLogging.setlogLevel(jobmonLogging._logLevel)
            jobmonLogging._logger.addHandler(jobmonLogging._handler)
            jobmonLogging._logger.setLevel(jobmonLogging._logLevel)

    @staticmethod
    def setlogLevel(level):
        jobmonLogging._logLevel = level
        jobmonLogging._handler.setLevel(level)
        jobmonLogging._logger.addHandler(jobmonLogging._handler)
        jobmonLogging._logger.setLevel(level)

    @staticmethod
    def getlogLevel():
        return jobmonLogging._logLevel

    @staticmethod
    def getLogger():
        if jobmonLogging._logger is None:
            jobmonLogging()
        return jobmonLogging._logger
"""

def main():
    """
    logger = jobmonLogging.getLogger()
    print(logging.getLevelName(jobmonLogging.getlogLevel()))
    logger.info("info")
    logger.debug("Debug")
    logger.warning("Warning")
    print()

    jobmonLogging.setlogLevel(logging.INFO)
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
    """
    logger = getLogger("lalala")
    logger.debug(getLogLevel())
    logger.debug("Debug")
    logger.info("Info")
    print()

    setLogLevel(logging.DEBUG)
    logger = getLogger("mimimi")
    logger.debug(getLogLevel())
    logger.debug("Debug")
    logger.info("Info")
    print()


if __name__ == "__main__":
    main()