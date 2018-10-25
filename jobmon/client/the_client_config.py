import os

from jobmon.client.config import ClientConfig, derive_jobmon_command_from_env
import jobmon


def get_the_client_config():
    return ClientConfig(
        jobmon_version=str(jobmon.__version__),
        host=os.environ.get('RUN_HOST', 'jobmon-p01.ihme.washington.edu'),
        jsm_port=6256,
        jqs_port=6258,
        jobmon_command=derive_jobmon_command_from_env())
