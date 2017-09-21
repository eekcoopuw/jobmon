from jobmon.connection_config import ConnectionConfig
from jobmon.exceptions import SGENotAvailable

try:
    from jobmon import sge

    conn_str = "mysql+pymysql://docker:docker@jobmon-p01.ihme.washington.edu/docker"

    jm_rep_conn = ConnectionConfig(
        host='jobmon-p01.ihme.washington.edu',
        port='3456')
    jm_pub_conn = ConnectionConfig(
        host='jobmon-p01.ihme.washington.edu',
        port='3457')

except SGENotAvailable:
    conn_str = "mysql://docker:docker@127.0.0.1/docker"

    jm_rep_conn = ConnectionConfig(
        host='localhost',
        port='3456')
    jm_pub_conn = ConnectionConfig(
        host='localhost',
        port='3457')
