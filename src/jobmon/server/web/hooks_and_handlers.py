"""Add handlers to deal with server-side exceptions and logging."""
from typing import Any

from flask import Flask, jsonify, request
from werkzeug.local import LocalProxy


from jobmon.server.web.log_config import get_logger, set_logger
from jobmon.server.web.server_side_exception import InvalidUsage, ServerError


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(get_logger)


def add_hooks_and_handlers(app: Flask) -> Flask:
    """Add logging hooks and exception handlers."""

    @app.errorhandler(Exception)
    def handle_anything(error: Any) -> Any:
        try:
            status_code = error.status_code
        except AttributeError:
            status_code = 500
        logger.exception(status_code=status_code)

        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = status_code
        return response

    # handle 404 at the application level not the blueprint level
    @app.errorhandler(404)
    def page_not_found(e: ServerError) -> tuple:
        return f'This route does not exist: {request.url}', 404

    # error handling
    @app.errorhandler(InvalidUsage)
    def handle_4xx(error: InvalidUsage) -> Any:
        logger.exception(status_code=error.status_code)

        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    # error handling
    @app.errorhandler(ServerError)
    def handle_5xx(error: ServerError) -> Any:
        logger.exception(status_code=error.status_code)

        response_dict = {"type": str(type(error)), "exception_message": str(error)}
        response = jsonify(error=response_dict)
        response.content_type = "application/json"
        response.status_code = error.status_code
        return response

    @app.before_request
    def add_requester_context() -> None:
        new_logger = logger.new()
        data = request.get_json() or {}
        if request.method == "GET":
            server_structlog_context = data
        if request.method in ["POST", "PUT"]:
            server_structlog_context = data.pop("server_structlog_context", {})
        if server_structlog_context:
            new_logger = new_logger.bind(path=request.path, **server_structlog_context)
        set_logger(new_logger)

    return app
