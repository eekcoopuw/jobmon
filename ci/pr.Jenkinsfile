pipeline {
  agent { label "qlogin" }
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
  } // End Options
  triggers {
    // This cron expression runs seldom, or never runs, but having the value set
    // allows bitbucket server to remotely trigger builds.
    // Git plugin 4.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/209059847/Triggering+Jenkins+on+new+Pull+Requests
    // Git plugin 3.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/955088898/Triggering+Jenkins+on+new+Pull+Requests+Git+Plugin+3.XX
    pollSCM ''
  }
  environment {

    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base &> /dev/null"
  }
  stages {
    stage("Notify BitBucket") {
      steps {
        // Tell BitBucket that a build has started.
        script {
          notifyBitbucket()
        }
      }
    }
    stage('Remote Checkout Repo') {
      steps {
        checkout scm
      }
    }
    stage("Lint") {
      steps {
        sh "${ACTIVATE} && nox --session lint || true"
      }
    }
    stage("Typecheck") {
      steps {
        sh "${ACTIVATE} && nox --session typecheck || true"
      }
    }
    stage("Build Docs") {
      steps {
        sh "${ACTIVATE} && nox --session docs"
      }
      post {
        always {
          // Publish the documentation.
          publishHTML([
            allowMissing: true,
            alwaysLinkToLastBuild: false,
            keepAll: true,
            reportDir: 'docsource/_build',
            reportFiles: '*',
            reportName: 'documentation',
            reportTitles: ''
          ])
        }
      }
    }
    stage('Tests') {
      steps {
        sh "${ACTIVATE} && nox --session tests -- ./tests/client/test_task.py"
      }
      post {
        always {
          // Publish the coverage reports.
          publishHTML([
            allowMissing: true,
            alwaysLinkToLastBuild: false,
            keepAll: true,
            reportDir: 'jobmon_coverage_html_report',
            reportFiles: '*',
            reportName: 'Coverage Report',
            reportTitles: ''
          ])
          // Publish the test results
          junit([
            testResults: "test_report.xml",
            allowEmptyResults: true
          ])
        }
      }
    }
  }
  post {
    always {
      // Delete the workspace directory.
      deleteDir()
    }
    failure {
      script {
        notifyBitbucket(buildStatus: 'FAILED')
      }
    }
    success {
      script {
        notifyBitbucket(buildStatus: 'SUCCESSFUL')
      }
    }
  }
}
