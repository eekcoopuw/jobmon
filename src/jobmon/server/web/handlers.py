"""Add handlers to deal with server-side exceptions and logging."""
import logging
import traceback
from typing import Dict, Optional

from elasticapm.contrib.flask import ElasticAPM

from flask import jsonify, request

from jobmon.log_config import configure_logger
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


def add_hooks_and_handlers(app, add_handlers: Optional[Dict] = None,
                           apm: Optional[ElasticAPM] = None):
    """Add logging hooks and exception handlers."""

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

    @app.errorhandler(Exception)
    def handle_anything(error):
        if apm is not None:
            apm.capture_exception(exc_info=(type(error), error, error.__traceback__))
        try:
            status_code = error.status_code
        except AttributeError:
            status_code = 500
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        app.logger.exception(response_dict, status_code=status_code)
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = status_code
        return response

    # handle 404 at the application level not the blueprint level
    @app.errorhandler(404)
    def page_not_found(e):
        return f'This route does not exist: {request.url}', 404

    # error handling
    @app.errorhandler(InvalidUsage)
    def handle_4xx(error):
        if apm is not None:
            apm.capture_exception(exc_info=(type(error), error, error.__traceback__))
        traceback.print_exc()
        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        app.logger.exception(response_dict, status_code=error.status_code)
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    # error handling
    @app.errorhandler(ServerError)
    def handle_5xx(error):
        if apm is not None:
            apm.capture_exception(exc_info=(type(error), error, error.__traceback__))
        traceback.print_exc()
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
        if request.method in ["POST", "PUT"]:
            server_structlog_context = data.pop("server_structlog_context", {})
        if server_structlog_context:
            app.logger = app.logger.bind(path=request.path, **server_structlog_context)

    return app
