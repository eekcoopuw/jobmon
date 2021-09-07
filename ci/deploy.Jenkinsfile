pipeline {
  agent none
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  } // End Options
  parameters {
    string(defaultValue: '',
     description: 'The version of Jobmon to deploy',
     name: 'JOBMON_VERSION')
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
    string(defaultValue: 'jobmon-reapers-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_REAPER_NAMESPACE')
    string(defaultValue: 'jobmon-dev-ips',
     description: 'Name of the MetalLB IP Pool you wish to get IPs from: https://stash.ihme.washington.edu/projects/ID/repos/metallb-scicomp/browse/k8s/scicomp-cluster-metallb.yml',
     name: 'METALLB_IP_POOL')
    string(defaultValue: 'jobmon-dev-db',
     description: 'name of rancher secret to use for db variables',
     name: 'RANCHER_DB_SECRET')
    string(defaultValue: 'True',
      description: 'Whether to forward event logs to Logstash or not',
      name: 'USE_LOGSTASH')
    string(defaultValue: 'jobmon-slack',
     description: 'name of rancher secret to use for slack variables',
     name: 'RANCHER_SLACK_SECRET')
    string(defaultValue: 'jobmon-qpid',
     description: 'name of rancher secret to use for qpid variables',
     name: 'RANCHER_QPID_SECRET')
    string(defaultValue: 'c-99499:p-4h54h',
     description: 'Rancher project must be created in the rancher web ui before running this job. Get this from the URL after you select the project in the rancher UI. Shouldnt change often',
     name: 'RANCHER_PROJECT_ID')
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to deploy Jobmon',
     name: 'DEPLOY_JOBMON')
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to deploy the ELK stack',
     name: 'DEPLOY_ELK')
  } // end parameters
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
    SCICOMP_DOCKER_REG_URL = "docker-scicomp.artifactory.ihme.washington.edu"
  } // end environment
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
                  docker image prune -f
                  get_metallb_cfg ${WORKSPACE}
               '''
          } // end credentials
          script {
            // TODO: more robust parsing script
            env.TARGET_IP = sh (
              script: '''#!/bin/bash
                         . ${WORKSPACE}/ci/deploy_utils.sh
                         get_metallb_ip_from_cfg "${METALLB_IP_POOL}" ${WORKSPACE}
                      ''',
              returnStdout: true
            ).trim()
          } // end script
          echo "Setting TARGET_IP=${env.TARGET_IP}"
        } // end node
      } // end steps
    } // end TARGETIP stage
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
          } // end script
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
          } // end credentials
        } // end node
      } // end steps
    } // end Build Containers stage
    stage ('Deploy K8s') {
      steps {
        node('docker') {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {

            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  deploy_jobmon_to_k8s \
                      ${WORKSPACE} \
                      ${JOBMON_CONTAINER_URI} \
                      ${METALLB_IP_POOL} \
                      ${K8S_NAMESPACE} \
                      ${RANCHER_PROJECT_ID} \
                      ${GRAFANA_CONTAINER_URI} \
                      ${RANCHER_DB_SECRET} \
                      ${RANCHER_SLACK_SECRET} \
                      ${RANCHER_QPID_SECRET} \
                      ${KUBECONFIG} \
                      ${USE_LOGSTASH} \
                      ${JOBMON_VERSION} \
                      ${K8S_REAPER_NAMESPACE} \
                      ${DEPLOY_JOBMON} \
                      ${DEPLOY_ELK}
               '''
          } // end credentials
        } // end node
      } // end steps
    } // end deploy k8s stage
    stage ('Test Deployment') {
      steps {
        node('qlogin') {
          // Download jobmon
          checkout scm
          // Download jobmonr
          sshagent (credentials: ['svcscicompci']) {
              sh "rm -rf jobmonr"
              sh "git clone ssh://git@stash.ihme.washington.edu:7999/scic/jobmonr.git"
           } // end sshagent

          echo "Workspace = ${WORKSPACE}"

          sh '''#!/bin/bash
                . ${WORKSPACE}/ci/deploy_utils.sh
                test_k8s_deployment \
                    ${WORKSPACE} \
                    "${QLOGIN_ACTIVATE}" \
                    ${JOBMON_VERSION} \
             ''' +  "${env.TARGET_IP}"
        } // end qlogin
      } // end steps
    } // end test deployment stage
    stage ('Create Shared Conda') {
      steps {
        node('qlogin') {
          sh '''. ${WORKSPACE}/ci/share_conda_install.sh \
                   /mnt/team/scicomp/pub/shared_jobmon_conda \
                   ${JOBMON_VERSION} \
                   /homes/svcscicompci/miniconda3/bin/conda
             '''
          } // end node
        } // end steps
      } // end create conda stage
    } // end stages
  post {
    always {
      node('docker') {
        // Delete the workspace directory.
        deleteDir()
      } // end node
      node('qlogin') {
        // Delete the workspace directory.
        deleteDir()
      } // end node
    } // end always
  } // end post
} // end pipeline