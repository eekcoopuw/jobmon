import os
from jobmon.server.deployment.util import Conf
from jobmon.server.deployment.build import BuildContainer


class JobmonDeployment(BuildContainer):

    def upload_image(self):
        os.system("docker push {}".format(self.tag))

def main():
    docker_file_dir = os.path.dirname(os.path.abspath(__file__)) + "/container"
    # Have to build under the jobmon root dir to install jobmon
    jobmon_root = os.path.dirname(os.path.abspath(__file__))[:0 - len("/jobmon/server/deployment")]
    JobmonDeployment(docker_file_dir, jobmon_root).upload_image()


if __name__ == "__main__":
    main()
