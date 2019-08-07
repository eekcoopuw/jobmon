import os
from jobmon.server.deployment.util import conf
from jobmon.server.deployment.buildContainer import BuildContainer


class JobmonDeployment(BuildContainer):

    def upload_image(self):
        os.system("docker push {}".format(self.tag))

    def deploy(self):
        self.build()
        if not conf.isTestMode():
            self.upload_image()


def main():
    JobmonDeployment().deploy()


if __name__ == "__main__":
    main()