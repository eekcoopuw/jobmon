import os

from jobmon.setup_config import SetupCfg as Conf
from jobmon.server.deployment.deploy import JobmonDeployment


def main():
    docker_file_dir = os.path.dirname(os.path.abspath(__file__)) + "/container"
    # Have to build under the jobmon root dir to install jobmon
    jobmon_root = os.path.dirname(os.path.abspath(__file__))[:0 - len("/jobmon/server/deployment")]
    jobmon = JobmonDeployment(docker_file_dir, jobmon_root)
    jobmon.build()
    if not Conf().is_test_mode():
        jobmon.upload_image()


if __name__ == "__main__":
    main()
