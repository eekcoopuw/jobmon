import logging
import traceback
from typing import Optional, Dict

from flask import jsonify, request

from jobmon.log_config import configure_logger
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


def add_hooks_and_handlers(app, add_handlers: Optional[Dict] = None):

    @app.before_first_request
    def setup_logging():
        app.logger = configure_logger("jobmon.server.web", add_handlers)

        # werkzeug logger
        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.disabled = True
        werkzeug_logger.setLevel(logging.ERROR)

        # sqlalchemy logger
        sqlalchemy_logger = logging.getLogger('sqlalchemy')
        sqlalchemy_logger.setLevel(logging.WARNING)

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
        print("foo")
        tb = error.__traceback__
        stack_trace = traceback.format_list(traceback.extract_tb(tb))
        print(stack_trace)
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        app.logger.exception(response_dict, status_code=error.status_code)
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    @app.before_request
    def log_request_info():
        app.logger = app.logger.new()
        app.logger = app.logger.bind(request_method=request.method)
        data = request.get_json() or {}
        if request.method == "GET":
            server_structlog_context = data
            app.logger = app.logger.bind(path=request.path, data=request.args)
        if request.method in ["POST", "PUT"]:
            server_structlog_context = data.pop("server_structlog_context", {})
            app.logger = app.logger.bind(path=request.path, data=data)
        if server_structlog_context:
            app.logger = app.logger.bind(**server_structlog_context)

    return app
