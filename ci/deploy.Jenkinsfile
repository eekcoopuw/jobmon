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
    string(defaultValue: 'jobmon-dev-db',
     description: 'name of rancher secret to use for db variables',
     name: 'RANCHER_DB_SECRET')
    string(defaultValue: 'jobmon-slack',
     description: 'name of rancher secret to use for slack variables',
     name: 'RANCHER_SLACK_SECRET')
    string(defaultValue: 'jobmon-qpid',
     description: 'name of rancher secret to use for qpid variables',
     name: 'RANCHER_QPID_SECRET')
    string(defaultValue: 'c-99499:p-4h54h',
     description: 'Rancher project must be created in the rancher web ui before running this job. Get this from the URL after you select the project in the rancher UI. Shouldnt change often',
     name: 'RANCHER_PROJECT_ID')
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
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
    SCICOMP_DOCKER_REG_URL = "docker-scicomp.artifactory.ihme.washington.edu"
    INFRA_PUB_REG_URL="docker-infrapub.artifactory.ihme.washington.edu"
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
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  docker image prune
                  get_metallb_cfg "${INFRA_PUB_REG_URL}/kubectl:latest" ${WORKSPACE}
               '''
          }
          script {
            // TODO: more robust parsing script
            env.TARGET_IP = sh (
              script: '''#!/bin/bash
                         . ${WORKSPACE}/ci/deploy_utils.sh
                         get_metallb_ip_from_cfg "${METALLB_IP_POOL}" ${WORKSPACE}
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
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  upload_python_dist \
                      ${WORKSPACE} \
                      ${TARGET_IP} \
                      $REG_USERNAME \
                      $REG_PASSWORD \
                      "${DOCKER_ACTIVATE}"
               '''
            script {
              env.JOBMON_VERSION = sh (
                script: '''
                        basename $(find ./dist/jobmon-*.tar.gz) | \
                        sed "s/jobmon-\\(.*\\)\\.tar\\.gz/\\1/"
                        ''',
                returnStdout: true
              ).trim()
            }
            echo "Jobmon Version=${env.JOBMON_VERSION}"
          }
        }
      }
    }
    stage ('Build Server Containers') {
      steps {
        node('docker') {
          script {
            env.JOBMON_CONTAINER_URI = sh (
              script: '''#!/bin/bash
                         . ${WORKSPACE}/ci/deploy_utils.sh
                         get_container_name_from_version \
                             ${JOBMON_VERSION} \
                             ${SCICOMP_DOCKER_REG_URL}
                      ''',
              returnStdout: true
            ).trim()
            env.GRAFANA_CONTAINER_URI = sh (
              script: 'echo ${SCICOMP_DOCKER_REG_URL}/${K8S_NAMESPACE}-grafana:${BUILD_NUMBER}',
              returnStdout: true
            ).trim()
          }
          echo "Server Container Images:\nJobmon=${env.JOBMON_CONTAINER_URI}\nGrafana=${env.GRAFANA_CONTAINER_URI}"
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'REG_USERNAME',
                                            passwordVariable: 'REG_PASSWORD')]) {
            // this builds a requirements.txt with the correct jobmon version number and uses
            // it to build a dockerfile for the jobmon services
            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  upload_jobmon_image \
                      ${JOBMON_VERSION} \
                      ${WORKSPACE} \
                      $REG_USERNAME \
                      $REG_PASSWORD \
                      ${SCICOMP_DOCKER_REG_URL} \
                      ${JOBMON_CONTAINER_URI} \
                      ${GRAFANA_CONTAINER_URI}
               '''
          }
        }
      }
    }
    stage ('Deploy K8s') {
      steps {
        node('docker') {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {

            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  deploy_jobmon_to_k8s \
                      "${INFRA_PUB_REG_URL}/yasha:latest" \
                      ${WORKSPACE} \
                      ${JOBMON_CONTAINER_URI} \
                      ${METALLB_IP_POOL} \
                      ${K8S_NAMESPACE} \
                      ${RANCHER_PROJECT_ID} \
                      ${GRAFANA_CONTAINER_URI} \
                      ${RANCHER_DB_SECRET} \
                      ${RANCHER_SLACK_SECRET} \
                      ${RANCHER_QPID_SECRET} \
                      "${INFRA_PUB_REG_URL}/kubectl:latest" \
                      ${KUBECONFIG}
               '''
          }
        }
      }
    }
    stage ('Test Deployment') {
      steps {
        node('qlogin') {
          checkout scm
          sh '''#!/bin/bash
                . ${WORKSPACE}/ci/deploy_utils.sh
                test_k8s_deployment \
                    ${WORKSPACE} \
                    "${QLOGIN_ACTIVATE}" \
                    ${JOBMON_VERSION}
             '''
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
      node('qlogin') {
        // Delete the workspace directory.
        deleteDir()
      }
    }
  }
}
