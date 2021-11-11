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
