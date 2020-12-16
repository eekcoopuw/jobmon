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

    // Jenkins commands run in separate processes, so need to activate the environment every
    // time we run pip, poetry, etc.
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
    stage('Clone Build Script & Set Vars') {
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
    stage('Tests') {
        sh "${ACTIVATE} && nox --session tests"
    }
  }
  post {
    always {
      // Publish the coverage reports and test results.
      publishHTML([
        allowMissing: true,
        alwaysLinkToLastBuild: false,
        keepAll: true,
        reportDir: 'jobmon_coverage_html_report',
        reportFiles: 'index.html',
        reportName: 'Coverage Report',
        reportTitles: ''
      ])
      junit([
        testResults: "**/*_test_report.xml",
        allowEmptyResults: true
      ])

      // Delete the workspace directory.
      deleteDir()

      // Tell BitBucket whether the build succeeded or failed.
      script {
        notifyBitbucket()
      }
    }
}
