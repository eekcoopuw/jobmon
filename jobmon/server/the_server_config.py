import os

from jobmon.server.config import ServerConfig
import jobmon


def get_the_server_config():
    if not os.environ.get('conn_str', None) or 'singularity' not in os.environ['conn_str']:
        raise ValueError("Conn_str is wrong. "
                         "Got {}".format(os.environ.get('conn_str', None)))
    return ServerConfig(
        jobmon_version=str(jobmon.__version__),
        conn_str=os.environ['conn_str'],
        slack_token=None,
        default_wf_slack_channel=None,
        default_node_slack_channel=None,
        verbose=False)
