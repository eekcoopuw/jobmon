"""Configure Logging for structlogs, syslog, etc."""
import logging.config
import socket
from typing import Any, Dict, MutableMapping, Optional

from pythonjsonlogger import jsonlogger
import structlog

from jobmon import __version__


def get_logstash_handler_config(
    logstash_host: str,
    logstash_port: Optional[int] = None,
    logstash_protocol: str = "",
    logstash_log_level: str = "DEBUG",
) -> Dict:
    """If using logstash, get the right config."""
    # Define the transport mechanism
    transport_protocol_map = {
        "TCP": "logstash_async.transport.TcpTransport",
        "UDP": "logstash_async.transport.UdpTransport",
        "Beats": "logstash_async.transport.BeatsTransport",
        "HTTP": "logstash_async.transport.HttpTransport",
    }

    transport_protocol = transport_protocol_map[logstash_protocol]

    handler_name = "logstash"
    hostname = socket.gethostname()
    handler_config = {
        handler_name: {
            "level": logstash_log_level.upper(),
            "class": "logstash_async.handler.AsynchronousLogstashHandler",
            "formatter": "json",
            "transport": transport_protocol,
            "host": logstash_host,
            "port": logstash_port,
            "database_path": f"/tmp/sqlite/logstash-{hostname}.db",
        }
    }
    return handler_config


def _processor_add_version(
    logger: logging.Logger, log_method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict["jobmon_version"] = __version__
    return event_dict


def _processor_remove_data_if_not_debug(
    logger: logging.Logger, log_method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    if "web" in logger.name and log_method != "debug":
        if "data" in event_dict.keys():
            event_dict.pop("data")
    return event_dict


def configure_logger(
    name: str, add_handlers: Optional[Dict] = None, json_formatter_prefix: str = ""
) -> None:
    """Configure logging format, handlers, etc."""
    dict_config: Dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            # copied formatter from here: https://github.com/hynek/structlog/issues/235
            "console": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.dev.ConsoleRenderer(),
                "keep_exc_info": True,
                "keep_stack_info": True,
            },
            "json": {"()": jsonlogger.JsonFormatter, "prefix": json_formatter_prefix},
        },
        # only stream handler by default. can add syslog using args
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "console",
            },
        },
        "loggers": {
            # only configure loggers of given name
            name: {
                "handlers": ["default"],
                "level": "DEBUG",
                "propagate": True,
            },
            "werkzeug": {
                "level": "WARN",
            },
            "sqlalchemy": {
                "level": "WARN",
            }
            # enable SQL debug
            # 'sqlalchemy.engine': {
            #     'level': 'INFO',
            # }
        },
    }

    if add_handlers is not None:
        dict_config["handlers"].update(add_handlers)
        handlers = dict_config["loggers"][name]["handlers"]
        handlers.extend([k for k in add_handlers.keys()])
        dict_config["loggers"][name]["handlers"] = list(set(handlers))

    logging.config.dictConfig(dict_config)
    structlog.configure(
        processors=[
            # bring in threadlocal context
            structlog.threadlocal.merge_threadlocal,
            # This performs the initial filtering, so we don't
            # evaluate e.g. DEBUG when unnecessary
            structlog.stdlib.filter_by_level,
            # remove 'data' key from logger if the log level isn't debug
            _processor_remove_data_if_not_debug,
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
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
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
        cache_logger_on_first_use=True,
    )
