pipeline {
  agent {
    label "docker"
  }
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  } // End Options
  parameters {
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
  } // end parameters
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    DOCKER_ACTIVATE = ". /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = ". /homes/svcscicompci/miniconda3/bin/activate base"
    MINICONDA_PATH = "/homes/svcscicompci/miniconda3/bin/activate"
    CONDA_ENV_NAME = "base"
  } // end environment
  stages {
    stage('Remote Checkout Repo') {
      steps {
        checkout scm
      } // End step
    } // End remote checkout repo stage
    stage('Get service configuration info') {
      steps {
        script {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {
            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  get_connection_info_from_namespace ${WORKSPACE} ${K8S_NAMESPACE}
               '''
          } // end credentials
        }
        script {
          env.JOBMON_SERVICE_FQDN = sh (
            script: '''#!/bin/bash
                       cat ${WORKSPACE}/jobmon_service_fqdn.txt
                    ''',
            returnStdout: true
          ).trim()
        } // end script
        script {
          env.JOBMON_SERVICE_PORT = sh (
            script: '''#!/bin/bash
                       cat ${WORKSPACE}/jobmon_service_port.txt
                    ''',
            returnStdout: true
          ).trim()
        } // end script
        echo "Setting JOBMON_SERVICE_FQDN=${env.JOBMON_SERVICE_FQDN}"
        echo "Setting JOBMON_SERVICE_PORT=${env.JOBMON_SERVICE_PORT}"
      } // end steps
    } // end TARGETIP stage
    stage('Build Docs via docker') {
      steps {
        script {
          sh '''#!/bin/bash
                export WEB_SERVICE_FQDN="${JOBMON_SERVICE_FQDN}"
                export WEB_SERVICE_PORT="${JOBMON_SERVICE_PORT}"
                ${DOCKER_ACTIVATE} nox --session docs
             '''
        } // End script
      } // end steps
    } // end build stage
  } // end stages
  post {
    always {
      // Delete the workspace directory.
      deleteDir()
    } // End always
  } // End post
} // End pipeline
