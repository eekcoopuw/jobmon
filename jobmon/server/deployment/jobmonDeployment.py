#!/usr/bin/env python

import os

from jobmon.server.deployment import util
from jobmon.server.deployment.util import conf
from jobmon.server.deployment.buildContainer import BuildContainer


class JobmonDeployment(BuildContainer):

    def _upload_image(self):
        os.system("docker push {}".format(self.tag))

    def deploy(self):
        self.build()
        if not conf.isTestMode():
            self._upload_image()


def main():
    version = conf.getJobmonVersion()
    tag = f"registry-app-p01.ihme.washington.edu/jobmon/jobmon:{version}"
    JobmonDeployment(tag).deploy()


if __name__ == "__main__":
    main()