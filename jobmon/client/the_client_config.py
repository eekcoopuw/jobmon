from jobmon.client.config import GlobalConfig, derive_jobmon_command_from_env
import jobmon


def get_the_client_config():
    if 'the_client_config' not in globals():
        global the_client_config
        the_client_config = GlobalConfig(
            jobmon_version=str(jobmon.__version__),
            host="jobmon-p01.ihme.washington.edu",
            jsm_port=5056,
            jqs_port=5058,
            jobmon_command=derive_jobmon_command_from_env())
    return the_client_config
