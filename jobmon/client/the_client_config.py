import os

from jobmon.client.config import ClientConfig, derive_jobmon_command_from_env
import jobmon


def get_the_client_config():
    if not os.environ.get('host', None) or 'ihme' not in os.environ['host']:
        raise ValueError("Host is wrong. "
                         "Got {}".format(os.environ.get('host', None)))
    return ClientConfig(
        jobmon_version=str(jobmon.__version__),
        host=os.environ['host'],
        jsm_port=5056,
        jqs_port=5058,
        jobmon_command=derive_jobmon_command_from_env())
