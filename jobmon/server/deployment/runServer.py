import os

from jobmon.server.deployment.util import conf
from jobmon.server.deployment.jobmonDeployment import JobmonDeployment
import jobmon.server.deployment.releaseNewVersion as releaseNewVersion

def main():
    releaseNewVersion.tag_release()
    JobmonDeployment().deploy()


if __name__ == "__main__":
    main()