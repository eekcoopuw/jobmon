import logging
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
        werkzeug_logger.addHandler(app.logger.handlers)

        # sqlalchemy logger
        sqlalchemy_logger = logging.getLogger('sqlalchemy')
        sqlalchemy_logger.setLevel(logging.WARNING)
        sqlalchemy_logger.addHandler(app.logger.handlers)

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
