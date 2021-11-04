pipeline {
  agent {
    label "qlogin"
  }
  triggers {
    bitBucketTrigger(
      [
        [
          $class: 'BitBucketPPRPullRequestServerTriggerFilter',
          actionFilter: [
            $class: 'BitBucketPPRPullRequestServerMergedActionFilter',
            allowedBranches: 'release/*'
          ]
        ],
        [
          $class: 'BitBucketPPRPullRequestServerTriggerFilter',
          actionFilter: [
            $class: 'BitBucketPPRPullRequestServerMergedActionFilter',
            allowedBranches: 'main'
          ]
        ]
      ]
    )
  } // end triggers.
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
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
  } // End options
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    ACTIVATE = ". /homes/svcscicompci/miniconda3/bin/activate base"
  } // End environment
  stages {
    stage("Notify BitBucket") {
      steps {
        // Tell BitBucket that a build has started.
        script {
          notifyBitbucket()
        } // End script
      } // End step
    } // End notify bitbucket stage
    stage('Remote Checkout Repo') {
      steps {
        checkout([
          $class: 'GitSCM',
          branches: [[name: params.BRANCH_TO_BUILD]],
          userRemoteConfigs: scm.userRemoteConfigs
        ])
      } // End step
    } // End remote checkout repo stage
    stage ('Build Python Distribution') {
      steps {
        script {
          // Optionally build Python package and publish to Pypi, conditioned on success of tests/lint/typecheck
          if (params.DEPLOY_PYPI) {
            // Artifactory user with write permissions
            withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                              usernameVariable: 'REG_USERNAME',
                                              passwordVariable: 'REG_PASSWORD')]) {
              sh '''#!/bin/bash
                    . ${WORKSPACE}/ci/deploy_utils.sh
                    upload_python_dist \
                        ${WORKSPACE} \
                        $REG_USERNAME \
                        $REG_PASSWORD \
                        "${ACTIVATE}"
                 '''
            } // end credentials
          } // end if params
          else {
            echo "Not deploying to Pypi"
          } // end else
        } // end script
      } // end steps
    } // end Build Python Distribution stage
  } // end stages
  post {
    always {
      // Delete the workspace directory.
      deleteDir()
    } // End always
    failure {
      script {
        notifyBitbucket(buildStatus: 'FAILED')
      } // End script
    } // End failure
    success {
      script {
        notifyBitbucket(buildStatus: 'SUCCESSFUL')
      } // End script
    } // End success
  } // End post
} // End pipeline
