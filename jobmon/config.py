from jobmon.connection_config import ConnectionConfig

conn_str = "mysql://docker:docker@db/docker"
jm_conn_obj = ConnectionConfig(
    monitor_host='localhost',
    monitor_port='4567')
