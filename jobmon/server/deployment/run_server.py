from jobmon.server.deployment.jobmon_deployment import JobmonDeployment
import jobmon.server.deployment.release_new_version as release_new_version


def main():
    release_new_version.tag_release()
    JobmonDeployment().deploy()


if __name__ == "__main__":
    main()
