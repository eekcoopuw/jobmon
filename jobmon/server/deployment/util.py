import random
import string
import subprocess
import requests
import configparser

from jobmon.models.attributes import constants

INTERNAL_DB_HOST = "db"
INTERNAL_DB_PORT = 3306
EXTERNAL_DB_HOST = constants.deploy_attribute["SERVER_QDNS"]
EXTERNAL_DB_PORT = constants.deploy_attribute["DB_PORT"]

EXTERNAL_SERVICE_HOST = constants.deploy_attribute["SERVER_QDNS"]
EXTERNAL_SERVICE_PORT = constants.deploy_attribute["SERVICE_PORT"]

DEFAULT_WF_SLACK_CHANNEL = 'jobmon-alerts'
DEFAULT_NODE_SLACK_CHANNEL = 'suspicious_nodes'
SLACK_API_URL = constants.deploy_attribute['SLACK_API_URL']

def gen_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))


def git_current_commit():
    return subprocess.check_output(
        'git rev-parse HEAD', shell=True, encoding='utf-8').strip()


def git_tags(branch="HEAD"):
    """Finds any tags pointing at the current commit"""
    return subprocess.check_output(
        'git --no-pager tag --points-at {}'.format(branch),
        shell=True,
        encoding='utf-8').split()


def find_release(tags: list):
    """Assumes that the release # can be found in a git tag of the form
    release-#... For example, if the current commit has a tag 'release-1.2.3,'
    we asusme that this deployment corresponds to jobmon version 1.2.3 and the
    function returns '1.2.3'. If no tag matching that form is found, returns
    'no-release'"""
    candidate_tags = [t for t in tags if 'release-' in t]
    if len(candidate_tags) == 1:
        return candidate_tags[0].replace('release-', '')
    elif len(candidate_tags) > 1:
        raise RuntimeError("Multiple release tags found. {}. Cannot deploy "
                           "without a definitive release "
                           "number.".format(candidate_tags))
    else:
        return None


def validate_slack_token(slack_token: str) -> bool:
    """
    Checks whether a given slack token is valid

    :param slack_token: A Slack Bot User OAuth Access Token
    :return: True if the token validates, False otherwise
    """
    resp = requests.post(
        SLACK_API_URL,
        headers={'Authorization': 'Bearer {}'.format(slack_token)})
    if resp.status_code != 200:
        print(f"Response returned a bad status code: {resp.status_code} "
              f"(Expected 200) with content {resp.json()}. "
              f"Retry with token or skip")
        return False
    return resp.json()['error'] != 'invalid_auth'


class Conf:
    __instance = None

    @staticmethod
    def _getInstance():
        if Conf.__instance is None:
            Conf.__instance = configparser.ConfigParser()
            Conf.__instance.read('CONFIG.INI')

    def __init__(self):
        Conf._getInstance()

    def is_test_mode(self):
        return Conf.__instance["basic values"]["test_mode"] == "True"

    def get_jobmon_version(self):
        return Conf._instance["basic values"]["jobmon_version"]

    def is_existed_db(self):
        if Conf.__instance["basic values"]["existing_db"] == "True":
            return True
        else:
            return False

    def get_internal_db_host(self):
        if self.is_existed_db():
            return Conf.__instance["existing db"]["internal_db_host"]
        else:
            return INTERNAL_DB_HOST

    def get_internal_db_port(self):
        if self.is_existed_db():
            return str(Conf.__instance["existing db"]["internal_db_port"])
        else:
            return str(INTERNAL_DB_PORT)

    def get_external_db_port(self):
        if self.is_existed_db():
            return str(Conf.__instance["existing db"]["external_db_port"])
        else:
            return str(EXTERNAL_DB_PORT)

    def get_external_db_host(self):
        return EXTERNAL_DB_HOST

    def get_external_service_host(self):
        return EXTERNAL_SERVICE_HOST

    def get_external_service_port(self):
        return str(EXTERNAL_SERVICE_PORT)

    def get_slack_token(self):
        return Conf.__instance["basic values"]["slack_token"]

    def get_wf_slack_channel(self):
        return Conf.__instance["basic values"]["wf_slack_channel"]

    def get_node_slack_channel(self):
        return Conf._instance["basic values"]["node_slack_channel"]

    def get_docker_compose_template(self):
        if self.is_existed_db():
            return "docker-compose.yml.existingdb"
        else:
            return "docker-compose.yml.newdb"

    def get_jobmon_service_user_pwd(self):
        return Conf.__instance["existing db"]["jobmon_pass_service_user"]

    def get_docker_tag(self):
        return f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{self.get_jobmon_version()}"

    def get_git_tag(self):
        return Conf.__instance["production mode"]["jobmon_git_tag=release"]