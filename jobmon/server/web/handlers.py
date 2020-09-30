import sys
import logging

from flask import request

from jobmon import __version__


def add_request_hooks(app):

    @app.before_first_request
    def setup_logging():
        # TODO: log level should be configurable via ENV variables. use gunicorn_logger.level
        # gunicorn_logger = logging.getLogger('gunicorn.error')

        # setup console handler
        log_formatter = logging.Formatter(
            '%(asctime)s [%(name)-12s] ' + __version__ + ' %(module)s %(levelname)-8s '
            '%(threadName)s: %(message)s'
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.INFO)

        # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
        app_logger = logging.getLogger(app.name)
        app_logger.setLevel(logging.INFO)
        app_logger.addHandler(console_handler)

        # werkzeug logger
        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.disabled = True
        werkzeug_logger.setLevel(logging.ERROR)
        werkzeug_logger.addHandler(console_handler)

        # sqlalchemy logger
        sqlalchemy_logger = logging.getLogger('sqlalchemy')
        sqlalchemy_logger.setLevel(logging.WARNING)
        sqlalchemy_logger.addHandler(console_handler)

    @app.before_request
    def log_request_info():
        if request.method == "GET":
            app.logger.info(f"URL Path is {request.path}. Args are {request.args}")
        if request.method in ["POST", "PUT"]:
            app.logger.info(f'URL Path is {request.path}. Data is: {request.get_json()}')

    return app
