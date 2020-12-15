pipeline {
  agent { label 'docker' }
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '50'))
  } // End Options
  triggers {
    // This cron expression runs seldom, or never runs, but having the value set
    // allows bitbucket server to remotely trigger builds.
    // Git plugin 4.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/209059847/Triggering+Jenkins+on+new+Pull+Requests
    // Git plugin 3.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/955088898/Triggering+Jenkins+on+new+Pull+Requests+Git+Plugin+3.XX
    pollSCM('0 0 1 1 0')
  }
  stages {
    stage('build') {
      steps {
        node('docker') {
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp', usernameVariable: 'DOCKER_REG_USERNAME', passwordVariable: 'DOCKER_REG_PASSWORD')]) {
            wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'xterm']) {
              // delete all tags and pull fresh list from origin
              sh '''git tag -l | xargs git tag -d'''
              checkout scm
              sh '''
              # Get Jenkins Common and Source it
              . ./common.sh || (echo Failed to import common.sh; exit 1)

              # check for tag on HEAD. If not container will be tagged 'undefined'.
              export CONTAINER_TAG=$(git name-rev --name-only --tags HEAD)
              export CONTAINER_NAME=jobmon
              export CONTAINER_IMAGE=${SCICOMP_REG_URL}/${CONTAINER_NAME}:${CONTAINER_TAG}

              build_push_container ${SCICOMP_REG_URL} \
                                   "${DOCKER_REG_USERNAME}" \
                                   "${DOCKER_REG_PASSWORD}" \
                                   "${CONTAINER_IMAGE}" \
                                   jobmon/server/deployment/container/Dockerfile

              # Write the container image info to disk, so we can read it back in  with groovy code
              docker_image_size "${CONTAINER_IMAGE}" > "${WORKSPACE}/container_image_size.txt"
              printf "${CONTAINER_IMAGE}" > "${WORKSPACE}/container_image_name.txt"
              '''
              script
              {

                IMAGE_NAME = "${sh(script:'cat container_image_name.txt', returnStdout: true)}"
                IMAGE_SIZE = "${sh(script:'cat container_image_size.txt', returnStdout: true)}"
                currentBuild.displayName = "#${BUILD_NUMBER}: ${IMAGE_NAME}"
                currentBuild.description = "'${IMAGE_NAME}' '${IMAGE_SIZE}'"
              } // End Script
            } // End color wrap
          } // End withCredentials
        } // End Node
      } // End Steps
    } // End Stage
  } // End Stages
} // End Pipeline
