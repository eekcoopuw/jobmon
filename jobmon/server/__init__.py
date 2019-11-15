from flask import Flask
from flask_cors import CORS

from jobmon.server.config import ServerConfig
from jobmon.server.server_logging import jobmonLogging


jobmonLogging.createLoggers()
logger = jobmonLogging.getLogger(__name__)
logger.info(jobmonLogging.myself())


def create_app(config=None):
    """Create a Flask app"""
    from jobmon.models import DB
    from jobmon.server.job_query_server.job_query_server import jqs
    from jobmon.server.job_state_manager.job_state_manager import jsm
    from jobmon.server.job_visualization_server.job_visualization_server \
        import jvs

    app = Flask(__name__)
    if config is None:
        config = ServerConfig.from_defaults()
    app.config['SQLALCHEMY_DATABASE_URI'] = config.conn_str
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 500}

    # register blueprints
    app.register_blueprint(jqs)
    app.register_blueprint(jsm)
    app.register_blueprint(jvs)

    # register app with flask-sqlalchemy DB
    DB.init_app(app)

    # enable CORS
    CORS(app)

    return app


app = create_app()
