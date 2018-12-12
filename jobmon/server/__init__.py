from flask import Flask
from flask_cors import CORS

from jobmon.server.config import ServerConfig


def create_app(config=None):
    """Create a Flask app"""
    from jobmon.models import DB
    from jobmon.server.job_query_server.job_query_server import jqs
    from jobmon.server.job_state_manager.job_state_manager import jsm

    app = Flask(__name__)
    if config is None:
        config = ServerConfig.from_defaults()
    app.config['SQLALCHEMY_DATABASE_URI'] = config.conn_str
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # register blueprints
    app.register_blueprint(jqs)
    app.register_blueprint(jsm)

    # register app with flask-sqlalchemy DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    return app


app = create_app()
