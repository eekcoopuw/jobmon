from jobmon.connection_config import ConnectionConfig
from jobmon.exceptions import SGENotAvailable


env = "inmem"

if env == "inmem":
    conn_str = "sqlite://"

    jm_rep_conn = ConnectionConfig(
        host='localhost',
        port='3456')
    jm_pub_conn = ConnectionConfig(
        host='localhost',
        port='3457')
    jqs_rep_conn = ConnectionConfig(
        host='localhost',
        port='3458')
else:
    conn_str = "mysql://docker:docker@127.0.0.1/docker"

    jm_rep_conn = ConnectionConfig(
        host='localhost',
        port='3456')
    jm_pub_conn = ConnectionConfig(
        host='localhost',
        port='3457')
    jqs_rep_conn = ConnectionConfig(
        host='localhost',
        port='3458')
