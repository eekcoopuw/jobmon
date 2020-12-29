pipeline {
  agent none
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  } // End Options
  parameters {
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
    string(defaultValue: 'jobmon-dev-ips',
     description: 'Name of the MetalLB IP Pool you wish to get IPs from: https://stash.ihme.washington.edu/projects/ID/repos/metallb-scicomp/browse/k8s/scicomp-cluster-metallb.yml',
     name: 'METALLB_IP_POOL')
    string(defaultValue: 'foo',
     description: 'name of configmap to use for server variables',
     name: 'K8S_CONFIGMAP')
  }
  triggers {
    // This cron expression runs seldom, or never runs, but having the value set
    // allows bitbucket server to remotely trigger builds.
    // Git plugin 4.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/209059847/Triggering+Jenkins+on+new+Pull+Requests
    // Git plugin 3.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/955088898/Triggering+Jenkins+on+new+Pull+Requests+Git+Plugin+3.XX
    pollSCM('0 0 1 1 0')
  }
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    ACTIVATE = ". /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    SCICOMP_DOCKER_REG_URL = "docker-scicomp.artifactory.ihme.washington.edu"

  }
  stages {
    stage ('Get TARGET_IP address') {
      steps {
        node('docker') {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {
            checkout scm
            // create .jobmon.ini
            sh '''#!/bin/bash
              # pull kubectl container
              INFRA_PUB_REG_URL="docker-infrapub.artifactory.ihme.washington.edu"
              KUBECTL_CONTAINER="$INFRA_PUB_REG_URL/kubectl:latest"
              docker pull $KUBECTL_CONTAINER

              # get metallb configmap
              . ${WORKSPACE}/ci/deploy.sh || (echo Failed to import deploy.sh; exit 1)
              docker run -t \
                --rm \
                -v ${KUBECONFIG}:/root/.kube/config \
                --mount type=bind,source="${WORKSPACE}",target=/data \
                $KUBECTL_CONTAINER  \
                  -n metallb-system \
                  get configmap config \
                  -o "jsonpath={.data.config}" > metallb.cfg
            '''
          }
          script {
            // TODO: more robust parsing script
            env.TARGET_IP = sh (
              script: '''#!/bin/bash
                # 4th line after entry is VIP.
                grep -A 4 "${METALLB_IP_POOL}" ${WORKSPACE}/metallb.cfg | \
                grep "\\- [0-9].*/[0-9]*" | \
                sed -e "s/  - \\(.*\\)\\/32/\\1/"
              ''',
              returnStdout: true
            ).trim()
          }
          echo "Setting TARGET_IP=${env.TARGET_IP}"
        }
      }
    }
    stage ('Build Python Distribution') {
      steps {
        node('docker') {
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'REG_USERNAME',
                                            passwordVariable: 'REG_PASSWORD')]) {

            sh '''#!/bin/bash
            INI=${WORKSPACE}/jobmon/.jobmon.ini
            rm $INI
            echo "[client]\nweb_service_fqdn=${TARGET_IP}\nweb_service_port=80" > $INI
            ${ACTIVATE} && nox --session distribute
            PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
            twine upload --repository-url $PYPI_URL \
                         --username $REG_USERNAME \
                         --password $REG_PASSWORD \
                         ./dist/*
            '''
            script {
              env.JOBMON_VERSION = sh (
                script: '''
                basename $(find ./dist/jobmon-*.tar.gz) | sed "s/jobmon-\\(.*\\)\\.tar\\.gz/\\1/"
                ''',
                returnStdout: true
              ).trim()
            }
            echo "Jobmon Version=${env.JOBMON_VERSION}"
          }
        }
      }
    }
    stage ('Build Server Container') {
      steps {
        node('docker') {
          script {
            env.CONTAINER_IMAGE = sh (
              script: '''#!/bin/bash
                # check if dev is in the version string and pick a container name based on that
                if [[ "$JOBMON_VERSION" =~ "dev" ]]
                then
                  CONTAINER_NAME="jobmon_dev"
                else
                  CONTAINER_NAME="jobmon"
                fi
                echo "$DOCKER_REG_URL/$CONTAINER_NAME:${JOBMON_VERSION}"
              ''',
              returnStdout: true
            ).trim()
          }
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'REG_USERNAME',
                                            passwordVariable: 'REG_PASSWORD')]) {
            // this builds a requirements.txt with the correct jobmon version number and uses
            // it to build a dockerfile for the jobmon services
            sh '''#!/bin/bash
            echo "jobmon==${JOBMON_VERSION}" > ${WORKSPACE}/requirements.txt
            docker login -u "$REG_USERNAME" -p "$REG_PASSWORD" "https://${SCICOMP_DOCKER_REG_URL}"
            docker build --no-cache -t "${CONTAINER_IMAGE}" -f ./deployment/k8s/Dockerfile .
            docker push "${CONTAINER_IMAGE}"
            '''
          }
          echo "Server Container Image=${env.CONTAINER_IMAGE}"
        }
      }
    }
    stage ('Deploy K8s') {
      steps {
        node('docker') {
          echo "Server Container Image=${env.CONTAINER_IMAGE}"
        }
      }
    }
  }
  post {
    always {
      node('docker') {
        // Delete the workspace directory.
        deleteDir()
      }
    }
  }
}
