import logging.config
import structlog
import socket
from typing import Optional, Dict

from pythonjsonlogger import jsonlogger

from jobmon import __version__


def get_rsyslog_handler_config(rsyslog_host: str, rsyslog_port: str, rsyslog_protocol: str,
                               rsyslog_log_level: str = "DEBUG"):
    # setup syslog handler config to use json
    if rsyslog_protocol == "TCP":
        p = socket.SOCK_STREAM
    else:
        p = socket.SOCK_DGRAM
    handler_name = "structured"
    handler_config = {
        handler_name:
        {
            "level": rsyslog_log_level.upper(),
            "class": "logging.handlers.SysLogHandler",
            "address": f"{rsyslog_host}:{rsyslog_port}",
            "socktype": p,
            "formatter": "json",
        }
    }
    return handler_config


def _processor_add_version(logger, log_method, event_dict):
    event_dict["jobmon_version"] = __version__
    return event_dict


def configure_logger(name, add_handlers: Optional[Dict] = None):
    dict_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "()": logging.Formatter,
                "fmt": "{timestamp} [{level} - {logger}] ({jobmon_version}): {message}",
                "style": '{'
            },
            "json": {
                "()": jsonlogger.JsonFormatter,
            },
        },
        # only stream handler by default. can add syslog using args
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
        },
        "loggers": {
            # only configure loggers of given name
            name: {
                "handlers": ["default"],
                "level": "DEBUG",
                "propagate": True,
            },
        }
    }

    if add_handlers is not None:
        dict_config["handlers"].update(add_handlers)
        handlers = dict_config["loggers"][name]["handlers"]
        handlers.extend([k for k in add_handlers.keys()])
        dict_config["loggers"][name]["handlers"] = list(set(handlers))

    logging.config.dictConfig(dict_config)
    structlog.configure(
        processors=[
            # This performs the initial filtering, so we don't
            # evaluate e.g. DEBUG when unnecessary
            structlog.stdlib.filter_by_level,
            # Adds logger=module_name (e.g __main__)
            structlog.stdlib.add_logger_name,
            # Adds level=info, debug, etc.
            structlog.stdlib.add_log_level,
            # add jobmon version to json object
            _processor_add_version,
            # Performs the % string interpolation as expected
            structlog.stdlib.PositionalArgumentsFormatter(),
            # add datetime to our logs
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            # Include the stack when stack_info=True
            structlog.processors.StackInfoRenderer(),
            # Include the exception when exc_info=True
            # e.g log.exception() or log.warning(exc_info=True)'s behavior
            structlog.processors.format_exc_info,
            # Creates the necessary args, kwargs for log()
            structlog.stdlib.render_to_log_kwargs,

        ],
        # Our "event_dict" is explicitly a dict
        # There's also structlog.threadlocal.wrap_dict(dict) in some examples
        # which keeps global context as well as thread locals
        context_class=dict,
        # Provides the logging.Logger for the underlaying log call
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Provides predefined methods - log.debug(), log.info(), etc.
        wrapper_class=structlog.stdlib.BoundLogger,
        # Caching of our logger
        cache_logger_on_first_use=True
    )
    return structlog.get_logger(name)
