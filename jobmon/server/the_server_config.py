import os

from jobmon.server.config import ServerConfig
import jobmon


def get_the_server_config():
    return ServerConfig(
        jobmon_version=str(jobmon.__version__),
        conn_str=os.environ.get(
            'CONN_STR', ("mysql://docker:docker@db:3306/docker")),
        slack_token=None,
        default_wf_slack_channel=None,
        default_node_slack_channel=None,
        verbose=False)
