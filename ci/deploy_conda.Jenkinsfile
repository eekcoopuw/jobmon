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
//     stage('Build Conda Distribution') {
//       steps {
//         script {
//           sh '''#!/bin/bash
//                 export PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
//                 export CONDA_CLIENT_VERSION="${CONDA_CLIENT_VERSION}"
//                 export JENKINS_BUILD_NUMBER="${BUILD_NUMBER}"
//                 export CONDA_CLIENT_VERSION="${CONDA_CLIENT_VERSION}"
//                 export JOBMON_VERSION="${JOBMON_VERSION}"
//                 export JOBMON_UGE_VERSION="${JOBMON_UGE_VERSION}"
//                 export JOBMON_SLURM_VERSION="${JOBMON_SLURM_VERSION}"
//                 export SLURM_REST_VERSION="${SLURM_REST_VERSION}"
//                 export WEB_SERVICE_FQDN="${JOBMON_SERVICE_FQDN}"
//                 export WEB_SERVICE_PORT="${JOBMON_SERVICE_PORT}"
//                 ${DOCKER_ACTIVATE} && nox --session conda_build
//              '''
//         } // End script
//       } // end steps
//     } // end build stage
//     stage('Upload Conda Distribution') {
//       steps {
//         script {
//           withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
//                                             usernameVariable: 'REG_USERNAME',
//                                             passwordVariable: 'REG_PASSWORD')]) {
//             sh '''#!/bin/bash
//                   FULL_FILEPATH=$(find "${WORKSPACE}/conda_build_output/noarch" -name "ihme_jobmon*.bz2")
//                   UPLOAD_FILEPATH=$(basename $FULL_FILEPATH)
//
//                   curl -XPUT \
//                     --user "${REG_USERNAME}:${REG_PASSWORD}" \
//                     -T ${FULL_FILEPATH} \
//                     "https://artifactory.ihme.washington.edu/artifactory/conda-scicomp/noarch/$UPLOAD_FILEPATH"
//                '''
//           } // end credentials
//         } // end script
//       } // end steps
//     } // end upload stage
    stage('Build Docs') {
      steps {
        script {
          sh '''#!/bin/bash
                export WEB_SERVICE_FQDN="${JOBMON_SERVICE_FQDN}"
                export WEB_SERVICE_PORT="${JOBMON_SERVICE_PORT}"
                ${DOCKER_ACTIVATE} && nox --session docs
             '''
        } // End script
      } // end steps
    } // end build docs stage
    stage('Upload Docs') {
      steps {
        script {
            sh '''#!/bin/bash
                cp -r ${WORKSPACE}/out/_html /mnt/team/scicomp/pub/html_test
               '''
        } // end script
        script {
            sh '''#!/bin/bash
                ls -al /mnt/team/scicomp/pub/html_test
               '''
        } // end script
        node('qlogin') {
          script {
            sh '''#!/bin/bash
              export WEB_SERVICE_FQDN="${JOBMON_SERVICE_FQDN}"
              export WEB_SERVICE_PORT="${JOBMON_SERVICE_PORT}"
              ${QLOGIN_ACTIVATE} && cp -r /mnt/team/scicomp/pub/html_test /ihme/centralcomp/docs/jobmon
            '''
          } // end script
        } // end qlogin
      } // end steps
    } // end upload doc stage
//     stage("parallel") {
//       parallel {
//         stage ('Test Conda Client UGE') {
//           steps {
//             node('qlogin') {
//               // Download jobmon
//               checkout scm
//               sh '''#!/bin/bash
//                     . ${WORKSPACE}/ci/deploy_utils.sh
//                     test_conda_client_uge \
//                         ${WORKSPACE} \
//                         "${QLOGIN_ACTIVATE}" \
//                         ${CONDA_CLIENT_VERSION} \
//                         ${JOBMON_VERSION} \
//                  ''' +  "${env.JOBMON_SERVICE_FQDN}"
//             } // end qlogin
//           } // end steps
//         } // end test deployment stage
//         stage ('Test Conda Client Slurm') {
//           steps {
//             node('qlogin') {
//
//               // Be very, very careful with nested quotes here, it is safest not to use them
//               // because ssh parses and recreates the remote string.
//               // For reference see https://unix.stackexchange.com/questions/212215/ssh-command-with-quotes
//               // Ubuntu uses dash, not bash. bash has the "source" command, dash does not.
//               // In dash you have to use the "." command instead
//               // Conceptually it would be possible to use "/bin/bash -c"
//               // but that requires quotes around the command. It is safer to just use "." rather
//               // than "source" in deploy_utils.sh and execute it with dash.
//               // Also, don't try to pass a whole command as a single argument because that also
//               // requires clever quoting. Only pass single words as arguments.
//
//               // Download jobmon
//               checkout scm
//               script {
//                 ssh_cmd= """. ${WORKSPACE}/ci/deploy_utils.sh
//                      test_conda_client_slurm \
//                          ${WORKSPACE} \
//                          ${MINICONDA_PATH} \
//                          ${CONDA_ENV_NAME} \
//                          ${CONDA_CLIENT_VERSION} \
//                          ${JOBMON_VERSION} \
//                          ${env.JOBMON_SERVICE_FQDN} \
//                 """
//                 echo ssh_cmd
//                 sshagent(['jenkins']) {
//                    sh "ssh -o StrictHostKeyChecking=no svcscicompci@gen-slurm-slogin-s01.cluster.ihme.washington.edu '${ssh_cmd}'"
//                 } // end ssh
//               } // end script
//             } // end qlogin
//           } // end steps
//         } // end test deployment stage
//       } // end parallel
//     } // end parallel stage
  } // end stages
  post {
    always {
      // Delete the workspace directory.
      deleteDir()
    } // End always
  } // End post
} // End pipeline
