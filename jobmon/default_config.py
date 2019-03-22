import os
from jobmon.models.attributes import constants


def derive_jobmon_command_from_env():
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return (
            'singularity run --app jobmon_command {}'
            .format(singularity_img_path).encode())
    return None


DEFAULT_SERVER_CONFIG = {
    "db_host": constants.deploy_attribute["SERVER_QDNS"],
    "db_port": constants.deploy_attribute["DB_PORT"],
    "db_user": "read_only",
    "db_pass": "docker",
    "slack_token": "",
    "wf_slack_channel": "",
    "node_slack_channel": "",
}

DEFAULT_CLIENT_CONFIG = {
    "host": constants.deploy_attribute["SERVER_QDNS"],
    "port": constants.deploy_attribute["SERVICE_PORT"],
    "jobmon_command": derive_jobmon_command_from_env(),
}
