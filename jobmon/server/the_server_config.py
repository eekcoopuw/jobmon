from jobmon.server.config import ServerConfig
import jobmon


def get_the_server_config():
    if 'the_server_config' not in globals():
        with open('/homes/cpinho/forked_jobmon/cfg.txt', 'w') as f:
            f.write("globals are {}".format(globals()))
        raise ValueError("shouldn't be in the real server config")
        global the_server_config
        the_server_config = ServerConfig(
            jobmon_version=str(jobmon.__version__),
            conn_str=("mysql://docker:docker@"
                      "jobmon-p01.ihme.washington.edu:3313/docker"),
            slack_token=None,
            default_wf_slack_channel=None,
            default_node_slack_channel=None,
            verbose=False)
    return the_server_config
