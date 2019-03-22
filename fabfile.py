from fabric.api import cd, env, run, settings
from jobmon.models.attributes import constants

env.use_ssh_config = True
env.hosts = [constants.deploy_attribute["HOSTNAME"]]


def deploy():
    with settings(warn_only=True):
        run("mkdir tmp")
        with cd("~/tmp"):
            run("rm -rf jobmon")

    with cd("~/tmp"):
        run("git clone ssh://git@stash.ihme.washington.edu:7999"
            "/~tomflem/jobmon.git")
        with cd("jobmon"):
            run("git checkout feature/comms_refactor")
            run("docker-compose -p jobmondev up --build --force-recreate -d")
