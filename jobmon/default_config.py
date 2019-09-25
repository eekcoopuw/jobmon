import os

from jobmon.setup_config import SetupCfg as Conf


def derive_jobmon_command_from_env():
    singularity_img_path = os.environ.get('IMGPATH', None)
    if singularity_img_path:
        return (
            'singularity run --app jobmon_command {}'
            .format(singularity_img_path).encode())
    return None


DEFAULT_SERVER_CONFIG = {
    "db_host": Conf().get_external_db_host(),
    "db_port": Conf().get_external_db_port(),
    "db_user": "read_only",
    "db_pass": "docker",
    "slack_token": Conf().get_slack_token(),
    "wf_slack_channel": Conf().get_wf_slack_channel(),
    "node_slack_channel": Conf().get_node_slack_channel(),
}

DEFAULT_CLIENT_CONFIG = {
    "host": Conf().get_external_service_host(),
    "port": Conf().get_external_service_port(),
    "jobmon_command": derive_jobmon_command_from_env(),
    "reconciliation_interval": int(Conf().get_reconciliation_interval()),
    "heartbeat_interval": int(Conf().get_heartbeat_interval()),
    "report_by_buffer": float(Conf().get_report_by_buffer())
}
