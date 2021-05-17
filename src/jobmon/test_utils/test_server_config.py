"""
Creates the Flask app and sqlalchemy database session for a single test.
Intended to be function scoped.
"""
try:
    from jobmon.server.web.models import DB
    from jobmon.server.web.api import WebConfig, create_app
except ImportError as e:
    error_msg = (
        "Only the dependencies necessary for running "
        "the Jobmon client are included in the requirements by default. "
        "If you are developing on Jobmon and want to run the tests, "
        "please also install the testing and server requirements.")
    raise Exception(error_msg) from e


def test_server_config(ephemera) -> dict:
    """
    This is run at the beginning of every test function to:
      1. tear down the db of the previous test and restart it fresh, and
      2. plus it starts all the services in this process

      HOWEVER, this flask application is ignored, we are forced to start it to get the
      database connection. This fixture is used to start the database, the services are
      ignored.

      Input:
          ephemera - a dict containing database connection information,
          specifically the database host, port, service user account,
          service user password, and the database name

      Returns:
          A dictionary with the flask app, sqlalchemy database session,
          and the Jobmon WebConfig object
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
