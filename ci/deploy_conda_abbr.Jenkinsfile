pipeline {
  agent {
    label "docker"
  }
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  } // End Options
  parameters {
    string(defaultValue: '',
     description: 'The version to be associated with this conda client release',
     name: 'CONDA_CLIENT_VERSION')
    string(defaultValue: '',
     description: 'The version of Jobmon Core',
     name: 'JOBMON_VERSION')
    string(defaultValue: '',
     description: 'The version of Jobmon UGE',
     name: 'JOBMON_UGE_VERSION')
    string(defaultValue: '',
     description: 'The version of Jobmon SLURM',
     name: 'JOBMON_SLURM_VERSION')
    string(defaultValue: '',
     description: 'The version of Slurm Rest',
     name: 'SLURM_REST_VERSION')
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
  } // end parameters
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
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
          sh (
            script: '''#!/bin/bash
                       cat ${WORKSPACE}
                       cat ${WORKSPACE}/jobmon_service_fqdn.txt
                       cp ${WORKSPACE}/jobmon_service_fqdn.txt /ihme/homes/samhu
                    ''',
            returnStdout: false
          )
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
  } // end stages
  post {
    always {
      // Delete the workspace directory.
      deleteDir()
    } // End always
  } // End post
} // End pipeline
