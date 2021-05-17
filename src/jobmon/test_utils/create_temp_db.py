"""Module to create a temporary ephemeradb in a Singularity or Docker container"""
import glob
import os
import re

try:
    from cluster_utils.ephemerdb import MARIADB, create_ephemerdb
    from sqlalchemy import create_engine
except ImportError as e:
    error_msg = (
        "Only the dependencies necessary for running "
        "the Jobmon client are included in the requirements by default. "
        "If you are developing on Jobmon and want to run the tests, "
        "please also install the testing and server requirements.")
    raise Exception(error_msg) from e


def create_temp_db() -> dict:
    """
    Boots a test ephemera database

    Returns:
      a dictionary with connection parameters
    """
    edb = create_ephemerdb(elevated_privileges=True, database_type=MARIADB)
    edb.db_name = "docker"
    conn_str = edb.start()

    # Set the time zone
    eng = create_engine(edb.root_conn_str)
    with eng.connect() as conn:
        conn.execute("SET GLOBAL time_zone = 'America/Los_Angeles'")

    # use the ephemera db root privileges (root: singularity_root) otherwise
    # you will not see changes to the database
    print(f"****** Database connection {conn_str}")

    # load schema
    here = os.path.dirname(__file__)
    create_dir = os.path.join(here, "db_schema")

    create_files = glob.glob(os.path.join(create_dir, "*.sql"))

    for file in sorted(create_files):
        edb.execute_sql_script(file)

    # get connection info
    pattern = ("mysql://(?P<user>.*):(?P<pass>.*)"
               "@(?P<host>.*):(?P<port>.*)/(?P<db>.*)")
    result = re.search(pattern, conn_str)
    db_conn_dict = result.groupdict()
    cfg = {
        "DB_HOST": db_conn_dict["host"],
        "DB_PORT": db_conn_dict["port"],
        "DB_USER": db_conn_dict["user"],
        "DB_PASS": db_conn_dict["pass"],
        "DB_NAME": db_conn_dict["db"]
    }
    return cfg
