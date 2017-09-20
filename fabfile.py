from fabric.api import cd, env, run, settings

env.use_ssh_config = True
env.hosts = ['jobmon-p01.ihme.washington.edu']


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
            run("docker-compose up --build -d")
