pipeline {
  agent {
    label "qlogin"
  }
  parameters {
    listGitBranches(
      branchFilter: '.*',
      credentialsId: 'jenkins',
      defaultValue: '${BITBUCKET_TARGET_BRANCH}',
      name: 'BRANCH_TO_BUILD',
      quickFilterEnabled: false,
      remoteURL: 'ssh://git@stash.ihme.washington.edu:7999/scic/jobmon.git',
      selectedValue: 'DEFAULT',
      sortMode: 'NONE',
      tagFilter: '*',
      type: 'PT_BRANCH'
    )
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
  }
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    ACTIVATE = ". /homes/svcscicompci/miniconda3/bin/activate base"
  } // End environment
  stages {
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
    stage('Remote Checkout Repo') {
      steps {
        checkout([
          $class: 'GitSCM',
          branches: [[name: params.BRANCH_TO_BUILD]],
          userRemoteConfigs: scm.userRemoteConfigs
        ])
      } // End step
    } // End remote checkout repo stage
    stage("parallel") {
      parallel {
        stage("Build Docs") {
          steps {
            sh "${ACTIVATE} && nox --session docs"
          } // End step
        } // End build docs stage
      } // End parallel
    } // End parallel stage
  } // end stages
  post {
    always {
      // Delete the workspace directory.
      deleteDir()
    } // End always
  } // End post
} // End pipeline
