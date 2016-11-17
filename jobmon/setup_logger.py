import logging
import time

#TODO reconcile this with the ame code in dalynator


def setup_logger(logger_name, log_file, level=logging.DEBUG):
    """Utility function to set up loggers, but only attach handlers if they are not already present"""
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logging.Formatter.converter = time.gmtime

    l.setLevel(level)
    # if logger already has handlers, don't add duplicate handlers
    if not l.handlers:
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)