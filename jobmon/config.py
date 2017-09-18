from jobmon.connection_config import ConnectionConfig

conn_str = "mysql://docker:docker@127.0.0.1/docker"

jm_rep_conn = ConnectionConfig(
    host='localhost',
    port='3456')
jm_pub_conn = ConnectionConfig(
    host='localhost',
    port='3457')
