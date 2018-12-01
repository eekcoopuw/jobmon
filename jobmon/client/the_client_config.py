import os

from jobmon.client.config import ClientConfig, derive_jobmon_command_from_env
import jobmon


def get_the_client_config():
    return ClientConfig(
        jobmon_version=str(jobmon.__version__),
        host=os.environ.get('JOBMON_HOST', 'jobmon-p01.ihme.washington.edu'),
        port=7256,
        jobmon_command=derive_jobmon_command_from_env())
