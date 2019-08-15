import os
from jobmon.server.deployment.util import Conf
from jobmon.server.deployment.build import BuildContainer


class JobmonDeployment(BuildContainer):

    def upload_image(self):
        os.system("docker push {}".format(self.tag))

def main():
    JobmonDeployment().upload_image()


if __name__ == "__main__":
    main()
