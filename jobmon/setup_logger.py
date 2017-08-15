import logging
from logging.config import dictConfig
import os

import yaml

# TODO reconcile this with the same code in dalynator

DEFAULT_CONFIG_FILE_PATH = "{}/client_logging.yaml".format(os.path.dirname(__file__))


def setup_logger(logger_name, path=DEFAULT_CONFIG_FILE_PATH,
                 default_level=logging.DEBUG, env_key="LOG_CFG"):
    """Utility function to set up loggers, but only attach handlers if they are
    not already present.  Looks for an environment value for the final_path, and then
    for a logging config file.
    
    Uses the singleton pattern so that handlers are only created once"""

    logger = logging.getLogger(logger_name)
    if logger.handlers:
        msg = "Ignoring duplicate log creation of {}".format(logger_name)
        logger.info(msg)
        return logger
    else:
        msg = "Will create logger for '{}'".format(logger_name)
        logger.info(msg)

    final_path = path
    value = os.getenv(env_key, None)

    if value:
        final_path = value
    if os.path.exists(final_path):
        with open(final_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        # Call into the logging package
        dictConfig(config)
        logger.info("Logging started for '{}', configured from file {}".format(logger_name, final_path))
    else:
        # fall back if it can't find the file
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(level=default_level, format=format)
        logger.info("Logging started for '{}', no logging config file found therefore using basicConfig".format(logger_name))

    return logger

