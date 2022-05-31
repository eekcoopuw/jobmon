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
    string(defaultValue: 'jobmon-slurm-sdb-dev',
     description: 'name of rancher secret to use for slurm_sdb variables',
     name: 'RANCHER_DB_SLURM_SDB_SECRET')
    string(defaultValue: 'c-99499:p-4h54h',
     description: 'Rancher project must be created in the rancher web ui before running this job. Get this from the URL after you select the project in the rancher UI. Shouldnt change often',
     name: 'RANCHER_PROJECT_ID')
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to deploy Jobmon',
     name: 'DEPLOY_JOBMON')
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to deploy the ELK stack',
     name: 'DEPLOY_ELK')
    booleanParam(defaultValue: 'true',
     description: 'Whether or not you want to deploy a new reaper to the reapers namespace',
     name: 'DEPLOY_REAPER')
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to config log rotation for elasticsearch',
     name: 'LOG_ROTATION')
  } // end parameters
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    SLURM_ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
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

            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  deploy_jobmon_to_k8s \
                      ${WORKSPACE} \
                      ${JOBMON_CONTAINER_URI} \
                      ${METALLB_IP_POOL} \
                      ${K8S_NAMESPACE} \
                      ${RANCHER_PROJECT_ID} \
                      ${RANCHER_DB_SECRET} \
                      ${RANCHER_SLACK_SECRET} \
                      ${RANCHER_QPID_SECRET} \
                      ${RANCHER_DB_SLURM_SDB_SECRET} \
                      ${KUBECONFIG} \
                      ${USE_LOGSTASH} \
                      ${JOBMON_VERSION} \
                      ${K8S_REAPER_NAMESPACE} \
                      ${DEPLOY_JOBMON} \
                      ${DEPLOY_ELK} \
                      ${DEPLOY_REAPER}
               '''
          } // end credentials
        } // end node
      } // end steps
    } // end deploy k8s stage
    stage('Get service configuration info') {
      steps {
        node('docker') {
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
        } // end node
      } // end steps
    } // end TARGETIP stage
    stage ('Create and apply log rotation ilm'){
      steps {
        script{
          node('docker') {
            if (LOG_ROTATION.toBoolean()) {
              // chang permission
              sh "chmod +x ${WORKSPACE}/ci/ilm/check_service_up.sh"

              // wait until elasticsearch is up
              sh '''/bin/bash ${WORKSPACE}/ci/ilm/check_service_up.sh ${JOBMON_SERVICE_FQDN}:9200'''

              // add a new policy to delete logs older than 8 days
              sh '''curl -XPUT "${JOBMON_SERVICE_FQDN}:9200/_ilm/policy/jobmon_ilm" --header "Content-Type: application/json" \
                 -d @${WORKSPACE}/ci/ilm/jobmon_ilm_policy.json
                 '''

              // delete index jobmon if it has been created by logstash
              // there may still be a race condition; but unable to trigger it on dev, so I don't know what happens when logstash reaches elasticsearch before template creation
              sh '''curl -X DELETE "${JOBMON_SERVICE_FQDN}:9200/jobmon" ||true'''

              // create a index template with the new policy
              sh '''curl -X PUT "${JOBMON_SERVICE_FQDN}:9200/_template/jobmon_template" --header "Content-Type: application/json" \
                 -d @${WORKSPACE}/ci/ilm/jobmon_index_template.json
              '''

              // create the first index
              sh '''curl -X PUT "${JOBMON_SERVICE_FQDN}:9200/jobmon-000001" --header "Content-Type: application/json" -d '{"aliases": {"jobmon": {"is_write_index": true}}}'
              '''
              // manage it by ilm
              sh '''curl -XPUT "${JOBMON_SERVICE_FQDN}:9200/jobmon-000001/_settings" -d '{"index":{"lifecycle.name":"jobmon_ilm","lifecycle.rollover_alias": "jobmon"}}' --header "Content-Type: application/json"
                 '''

              // set all index replica to 0 to get rid of the "yellow" warning in GUI because we only have one elasticsearch node
              sh '''curl -XPUT "${JOBMON_SERVICE_FQDN}:9200/*/_settings" --header "Content-Type: application/json" -d '{"index":{"number_of_replicas":0}}'
                 '''
            }
          else {
            sh "echo \"Skip log rotation configuration.\""
          } //else
        } //node
      } //script
    } //steps
   } //stage
    stage ('Test Deployment') {
      steps {
        node('slurm') {
          // Download jobmon
          checkout scm
          sh '''#!/bin/bash
                . ${WORKSPACE}/ci/deploy_utils.sh
                test_server \
                    ${WORKSPACE} \
                    "${SLURM_ACTIVATE}" \
                    ${JOBMON_VERSION} \
                    ${JOBMON_SERVICE_FQDN} \
                    ${JOBMON_SERVICE_PORT}
             '''
        } // end slurm
      } // end steps
    } // end test deployment stage
  } // end stages
  post {
    always {
      node('docker') {
        // Delete the workspace directory.
        deleteDir()
      } // end node
      node('slurm') {
        // Delete the workspace directory.
        deleteDir()
      } // end slurm
    } // end always
  } // end post
} // end pipeline
