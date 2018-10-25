import logging
import logging.config


def add_jobmon_file_logger(name, level, log_file_name):
    """ Adds a log file handler for Jobmon logging. """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    handler = logging.FileHandler(log_file_name)
    handler.setLevel(level)

    # create a logging format in local (Seattle) time
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    # Also write to stdout in case logging is broken.
    msg = ("'{}' Jobmon logger created at {}, ".format(name,
                                                       log_file_name))
    logger.info(msg)
    print(msg)

    return logging.getLogger(name)
