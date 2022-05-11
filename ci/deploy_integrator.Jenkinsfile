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
    string(defaultValue: 'jobmon-integrator-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
    string(defaultValue: 'jobmon-dev-db',
     description: 'name of rancher secret to use for db variables',
     name: 'RANCHER_DB_SECRET')
    string(defaultValue: 'jobmon-slurm-sdb-dev',
     description: 'name of rancher secret to use for db variables',
     name: 'RANCHER_DB_SLURM_SDB_SECRET')
    integer(defaultValue: 0,
     description: 'How many times should the integrator try to get the resource usage value before giving up. Put 0 to try forever.',
     name: 'INTEGRATOR_RETIRE_AGE')
    string(defaultValue: 'c-99499:p-4h54h',
     description: 'Rancher project must be created in the rancher web ui before running this job. Get this from the URL after you select the project in the rancher UI. Shouldnt change often',
     name: 'RANCHER_PROJECT_ID')
  } // end parameters
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
    SCICOMP_DOCKER_REG_URL = "docker-scicomp.artifactory.ihme.washington.edu"
    SCICOMP_DOCKER_DEV_URL = "docker-scicomp-dev.artifactory.ihme.washington.edu"
  } // end environment
  stages {
    stage('Remote Checkout Repo') {
      steps {
        node('docker') {
          checkout scm
        } // end node
      } // End step
    } // End remote checkout repo stage
    stage ('Build Server Containers') {
      steps {
        node('docker') {
          script {
            env.JOBMON_CONTAINER_URI = sh (
              script: '''#!/bin/bash
                         . ${WORKSPACE}/ci/deploy_utils.sh
                         get_container_name_from_version \
                             ${JOBMON_VERSION} \
                             ${SCICOMP_DOCKER_REG_URL} \
                             ${SCICOMP_DOCKER_DEV_URL}
                      ''',
              returnStdout: true
            ).trim()
          } // end script
          echo "Server Container Images:\nJobmon=${env.JOBMON_CONTAINER_URI}"
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'REG_USERNAME',
                                            passwordVariable: 'REG_PASSWORD')]) {
            // this builds a requirements.txt with the correct jobmon version number and uses
            // it to build a dockerfile for the jobmon services
            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  upload_jobmon_image \
                      "${JOBMON_VERSION}" \
                      ${WORKSPACE} \
                      $REG_USERNAME \
                      $REG_PASSWORD \
                      ${JOBMON_CONTAINER_URI}
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
            sh '''echo \
                  deploy_integrator_to_k8s \
                      ${WORKSPACE} \
                      ${JOBMON_CONTAINER_URI} \
                      ${K8S_NAMESPACE} \
                      ${RANCHER_PROJECT_ID} \
                      ${RANCHER_DB_SECRET} \
                      ${RANCHER_DB_SLURM_SDB_SECRET} \
                      ${KUBECONFIG} \
                      ${INTEGRATOR_RETIRE_AGE}
               '''
            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  deploy_integrator_to_k8s \
                      ${WORKSPACE} \
                      ${JOBMON_CONTAINER_URI} \
                      ${K8S_NAMESPACE} \
                      ${RANCHER_PROJECT_ID} \
                      ${RANCHER_DB_SECRET} \
                      ${RANCHER_DB_SLURM_SDB_SECRET} \
                      ${KUBECONFIG} \
                      ${INTEGRATOR_RETIRE_AGE}

               '''
          } // end credentials
        } // end node
      } // end steps
    } // end deploy k8s stage
  } // end stages
  post {
    always {
      node('docker') {
        // Delete the workspace directory.
        deleteDir()
      } // end node
    } // end always
  } // end post
} // end pipeline
