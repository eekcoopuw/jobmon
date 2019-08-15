from jobmon.server.deployment.deploy import JobmonDeployment
from jobmon.server.deployment.util import Conf


def main():
    jobmon = JobmonDeployment()
    jobmon.build()
    if not Conf().is_test_mode():
        jobmon.upload_image()


if __name__ == "__main__":
    main()
