import os


def derive_jobmon_command_from_env():
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return (
            'singularity run --app jobmon_command {}'
            .format(singularity_img_path).encode())
    return None


DEFAULT_SERVER_CONFIG = {
    "db_host": "jobmon-p01.ihme.washington.edu",
    "db_user": "read_only",
    "db_pass": "docker",
    "slack_token": "",
    "wf_slack_channel": "",
    "node_slack_channel": "",
}

DEFAULT_CLIENT_CONFIG = {
    "host": "jobmon-p01.ihme.washington.edu",
    "jobmon_command": derive_jobmon_command_from_env(),
}
