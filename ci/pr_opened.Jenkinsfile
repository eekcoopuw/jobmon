pipeline {
  agent {
    label "qlogin"
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
    stage('Merge Branches') {
      steps {
        checkout scm
      } // End step
    } // End remote checkout repo stage
    stage("parallel") {
      parallel {
        stage("Lint") {
          steps {
            sh "${ACTIVATE} && nox --session lint"
          } // End step
        } // End lint stage
        stage("Typecheck") {
          steps {
            sh "${ACTIVATE} && nox --session typecheck"
          } // End step
        } // End typecheck stage
        stage("Build Docs") {
          steps {
            sh "${ACTIVATE} && nox --session docs"
          } // End step
          post {
            always {
              // Publish the documentation.
              publishHTML([
                allowMissing: true,
                alwaysLinkToLastBuild: false,
                keepAll: true,
                reportDir: 'out/_html',
                reportFiles: 'index.html',
                reportName: 'Documentation',
                reportTitles: ''
              ])
            } // End always
          } // End post
        } // End build docs stage
        stage('Tests') {
          steps {
            sh "${ACTIVATE} && nox --session tests -- tests/ -n 3"
          }
          post {
            always {
              // Publish the coverage reports.
              publishHTML([
                allowMissing: true,
                alwaysLinkToLastBuild: false,
                keepAll: true,
                reportDir: 'jobmon_coverage_html_report',
                reportFiles: 'index.html',
                reportName: 'Coverage Report',
                reportTitles: ''
              ])
              // Publish the test results
              junit([
                testResults: "test_report.xml",
                allowEmptyResults: true
              ])
            } // End always
          } // End post
        } // End tests stage
      } // End parallel
    } // End parallel stage
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
