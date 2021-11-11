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
  }
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    ACTIVATE = ". /homes/svcscicompci/miniconda3/bin/activate base"
  } // End environment
  stages {
    stage('Check out Target Ip/Port') {
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
    }
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
