import os

from jobmon.client.config import ClientConfig, derive_jobmon_command_from_env
import jobmon


def get_the_client_config():
    return ClientConfig(
        jobmon_version=str(jobmon.__version__),
        host=os.environ.get('host', '0.0.0.0'),
        jsm_port=5256,
        jqs_port=5258,
        jobmon_command=derive_jobmon_command_from_env())
