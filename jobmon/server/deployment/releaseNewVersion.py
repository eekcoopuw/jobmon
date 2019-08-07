import os

from jobmon.server.deployment.util import conf
from jobmon.server.deployment.jobmonDeployment import JobmonDeployment


def tag_release():
    tag = conf.getGitTag()
    os.system(f"git tag -a {tag}")


def main():
    tag_release()
    JobmonDeployment().upload_image()


if __name__ == "__main__":
    main()