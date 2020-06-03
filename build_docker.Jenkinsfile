pipeline {
  agent { label 'master' }
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '50'))
  } // End Options
  triggers { pollSCM('0 0 1 1 0') }
  stages {
    stage('build') {
      steps {
        node('master') {
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp', usernameVariable: 'DOCKER_REG_USERNAME', passwordVariable: 'DOCKER_REG_PASSWORD')]) {
            wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'xterm']) {
              checkout scm
              sh '''
              # Get Jenkins Common and Source it
              . ./common.sh || (echo Failed to import common.sh; exit 1)

              export CONTAINER_NAME=jobmon
              export CONTAINER_TAG=guppy_${BUILD_NUMBER}_$(date +%Y%m%d_%H%M%S)
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
