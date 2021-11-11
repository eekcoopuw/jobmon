pipeline {
  agent {
    label "docker"
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
    DOCKER_ACTIVATE = "source /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
    MINICONDA_PATH = "/homes/svcscicompci/miniconda3/bin/activate"
    CONDA_ENV_NAME = "base"
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
            sh "${QLOGIN_ACTIVATE} && nox --session docs"
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
