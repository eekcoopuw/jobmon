import logging
import sys
import traceback

from flask import jsonify, request

from jobmon import __version__
from jobmon.log_config import configure_logger
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


def add_hooks_and_handlers(app, use_rsyslog, rsyslog_host, rsyslog_port, rsyslog_protocol):

    @app.before_first_request
    def setup_logging():
        # TODO: log level should be configurable via ENV variables. use gunicorn_logger.level
        # gunicorn_logger = logging.getLogger('gunicorn.error')

        app.logger = configure_logger("server", use_rsyslog, rsyslog_host, rsyslog_port,
                                      rsyslog_protocol)
        # setup console handler
        log_formatter = logging.Formatter(
            '%(asctime)s [%(name)-12s] ' + str(__version__) + ' %(module)s %(levelname)-8s '
            '%(threadName)s: %(message)s'
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.INFO)

        # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
        app_logger = logging.getLogger(app.name)
        app_logger.setLevel(logging.INFO)
        app_logger.addHandler(console_handler)
        #
        # werkzeug logger
        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.disabled = True
        werkzeug_logger.setLevel(logging.ERROR)
        werkzeug_logger.addHandler(console_handler)

        # sqlalchemy logger
        sqlalchemy_logger = logging.getLogger('sqlalchemy')
        sqlalchemy_logger.setLevel(logging.WARNING)
        sqlalchemy_logger.addHandler(console_handler)

        # handle 404 at the application level not the blueprint level

    @app.errorhandler(404)
    def page_not_found(e):
        return f'This route does not exist: {request.url}', 404

    # error handling
    @app.errorhandler(InvalidUsage)
    def handle_4xx(error):
        # tb = error.__traceback__
        # stack_trace = traceback.format_list(traceback.extract_tb(tb))
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        app.logger.exception(response_dict, status_code=error.status_code)
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    # error handling
    @app.errorhandler(ServerError)
    def handle_5xx(error):
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        app.logger.exception(response_dict, status_code=error.status_code)
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    @app.before_request
    def log_request_info():
        if request.method == "GET":
            app.logger.info(f"URL Path is {request.path}. Args are {request.args}")
        if request.method in ["POST", "PUT"]:
            app.logger.info(f'URL Path is {request.path}. Data is: {request.get_json()}')

    return app
