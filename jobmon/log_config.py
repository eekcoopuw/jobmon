import configargparse
import logging.config
import structlog
import socket
from pythonjsonlogger import jsonlogger

from jobmon import __version__
from jobmon.config import CLI, ParserDefaults


def configure_logger(name, use_rsyslog: bool = False, rsyslog_host: str = None,
                     rsyslog_port: str = None, rsyslog_protocol: str = None):
    if use_rsyslog:
        if rsyslog_protocol == "TCP":
            p = socket.SOCK_STREAM
        else:
            p = socket.SOCK_DGRAM
        struct_dict = {
                "level": "DEBUG",
                "class": "logging.handlers.SysLogHandler",
                "address": f"{rsyslog_host}:{rsyslog_port}",
                "socktype": p,
                "formatter": "json",
            }
    else:
        struct_dict = {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "json",
            }
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "()": logging.Formatter,
                "fmt": "{timestamp} [{level} - {logger}]: {message}",
                "style": '{'
            },
            "json": {
                "()": jsonlogger.JsonFormatter,
            },
        },
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
            "structured": struct_dict,
        },
        "loggers": {
            "": {
                "handlers": ["default", "structured"],
                "level": "DEBUG",
                "propagate": True,
            },
        }
    })
    structlog.configure(
        processors=[
            # This performs the initial filtering, so we don't
            # evaluate e.g. DEBUG when unnecessary
            structlog.stdlib.filter_by_level,
            # Adds logger=module_name (e.g __main__)
            structlog.stdlib.add_logger_name,
            # Adds level=info, debug, etc.
            structlog.stdlib.add_log_level,
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
    return structlog.get_logger(name, jobmon_version=str(__version__))
