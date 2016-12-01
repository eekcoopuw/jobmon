import logging
from logging.config import dictConfig
import os

import yaml


# TODO reconcile this with the same code in dalynator


def setup_logger(logger_name, default_path="logging.yaml",
                 default_level=logging.DEBUG, env_key="LOG_CFG"):
    """Utility function to set up loggers, but only attach handlers if they are
    not already present.  Looks for an environment value for the path, and then
    for a logging config file"""
    path = default_path
    value = os.getenv(env_key, None)

    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        dictConfig(config)
    else:
        # fall back if it can't find the file
        logging.basicConfig(level=default_level)

    return logging.getLogger(logger_name)

    # l = logging.getLogger(logger_name)
    # formatter = logging.Formatter(
    #     '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    # fileHandler = logging.FileHandler(log_file, mode='w')
    # fileHandler.setFormatter(formatter)
    # streamHandler = logging.StreamHandler()
    # streamHandler.setFormatter(formatter)
    # logging.Formatter.converter = time.gmtime
    #
    # l.setLevel(level)
    # # if logger already has handlers, don't add duplicate handlers
    # if not l.handlers:
    #     l.addHandler(fileHandler)
    #     l.addHandler(streamHandler)
