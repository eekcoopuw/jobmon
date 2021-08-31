"""Creates the Flask app and sqlalchemy database session for a single test.

Intended to be function scoped.
"""
try:
    from jobmon.server.web.api import WebConfig, create_app
    from jobmon.server.web.models import DB
except ImportError as e:
    error_msg = (
        "Only the dependencies necessary for running "
        "the Jobmon client are included in the requirements by default. "
        "If you are developing on Jobmon and want to run the tests, "
        "please also install the testing and server requirements.")
    raise Exception(error_msg) from e


def test_server_config(ephemera: dict) -> dict:
    """Create a webconfig and the create a Flask app from the webconfig.

    Args:
        ephemera: a dict containing database connection information, specifically the database
        host, port, service user account, service user password, and the database name.

    Returns:
        A dictionary with the flask app, sqlalchemy database session, and the Jobmon WebConfig
        object.
    """
    # The create_app call sets up database connections
    web_config = WebConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"])
    app = create_app(web_config)

    return {'app': app, 'DB': DB, "server_config": web_config}
