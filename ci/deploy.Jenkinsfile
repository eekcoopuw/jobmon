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
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to config log rotation for elasticsearch',
     name: 'LOG_ROTATION')
  } //end parameters

  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    // Build up QLOGIN_ACITVATE from separate words so that the individual words can be passed
    // separately.
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    MINICONDA_PATH = "/homes/svcscicompci/miniconda3/bin/activate"
    CONDA_ENV_NAME = "base"
    QLOGIN_ACTIVATE = ". ${MINICONDA_PATH} ${CONDA_ENV_NAME}"
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

    stage ('Create and apply log rotation ilm'){
      steps {
        script{
          node('docker') {
            if (LOG_ROTATION.toBoolean()) {
              // chang permission
              sh "chmod +x ${WORKSPACE}/ci/ilm/check_service_up.sh"

              // wait until elasticsearch is up
              sh '''/bin/bash ${WORKSPACE}/ci/ilm/check_service_up.sh ${TARGET_IP}:9200'''

              // add a new policy to delete logs older than 8 days
              sh '''curl -XPUT "${TARGET_IP}:9200/_ilm/policy/jobmon_ilm" --header "Content-Type: application/json" \
                 -d @${WORKSPACE}/ci/ilm/jobmon_ilm_policy.json
                 '''

              // delete index jobmon if it has been created by logstash
              // there may still be a race condition; but unable to trigger it on dev, so I don't know what happens when logstash reaches elasticsearch before template creation
              sh '''curl -X DELETE "${TARGET_IP}:9200/jobmon" ||true'''

              // create a index template with the new policy
              sh '''curl -X PUT "${TARGET_IP}:9200/_template/jobmon_template" --header "Content-Type: application/json" \
                 -d @${WORKSPACE}/ci/ilm/jobmon_index_template.json
              '''

              // create the first index
              sh '''curl -X PUT "${TARGET_IP}:9200/jobmon-000001" --header "Content-Type: application/json" -d '{"aliases": {"jobmon": {"is_write_index": true}}}'
              '''
              // manage it by ilm
              sh '''curl -XPUT "${TARGET_IP}:9200/jobmon-000001/_settings" -d '{"index":{"lifecycle.name":"jobmon_ilm","lifecycle.rollover_alias": "jobmon"}}' --header "Content-Type: application/json"
                 '''

              // set all index replica to 0 to get rid of the "yellow" warning in GUI because we only have one elasticsearch node
              sh '''curl -XPUT "${TARGET_IP}:9200/*/_settings" --header "Content-Type: application/json" -d '{"index":{"number_of_replicas":0}}'
                 '''
            }
          else {
            sh "echo \"Skip log rotation configuration.\""
          } //else
        } //node
      } //script
    } //steps
   } //stage
    stage ('Test UGE Deployment') {
          steps {
            node('qlogin') {
              // Download jobmon
              checkout scm
              // Download jobmonr
              sshagent (credentials: ['svcscicompci']) {
                  sh "rm -rf jobmonr"
                  sh "git clone ssh://git@stash.ihme.washington.edu:7999/scic/jobmonr.git"
               } // end sshagent
              sh '''#!/bin/bash
                    . ${WORKSPACE}/ci/deploy_utils.sh
                    test_k8s_uge_deployment \
                        ${WORKSPACE} \
                        "${QLOGIN_ACTIVATE}" \
                        ${JOBMON_VERSION} \
                 ''' +  "${env.TARGET_IP}"
            } // end qlogin
          } // end steps
        } // end test deployment stage
        stage ('Test Slurm Deployment') {
          steps {
            node('qlogin') {

              // Be very, very careful with nested quotes here, it is safest not to use them
              // because ssh parses and recreates the remote string.
              // For reference see https://unix.stackexchange.com/questions/212215/ssh-command-with-quotes
              // Ubuntu uses dash, not bash. bash has the "source" command, dash does not.
              // In dash you have to use the "." command instead
              // Conceptually it would be possible to use "/bin/bash -c"
              // but that requires quotes around the command. It is safer to just use "." rather
              // than "source" in deploy_utils.sh and execute it with dash.
              // Also, don't try to pass a whole command as a single argument because that also
              // requires clever quoting. Only pass single words as arguments.

              // Download jobmon
              checkout scm
              script {
                ssh_cmd= """. ${WORKSPACE}/ci/deploy_utils.sh
                     test_k8s_slurm_deployment \
                         ${WORKSPACE} \
                         ${MINICONDA_PATH} \
                         ${CONDA_ENV_NAME} \
                         ${JOBMON_VERSION} \
                         ${env.TARGET_IP} \
                """
                echo ssh_cmd
                sshagent(['jenkins']) {
                   sh "ssh -o StrictHostKeyChecking=no svcscicompci@gen-slurm-slogin-s01.hosts.ihme.washington.edu '${ssh_cmd}'"
                }
              }
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