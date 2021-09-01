pipeline {
  agent {
    label "qlogin"
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
  } // End options
  parameters {
    booleanParam(defaultValue: 'false',
     description: 'Whether or not you want to deploy Jobmon to Pypi',
     name: 'DEPLOY_PYPI')
  } // end parameters
  triggers {
    // This cron expression runs seldom, or never runs, but having the value set
    // allows bitbucket server to remotely trigger builds.
    // Git plugin 4.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/209059847/Triggering+Jenkins+on+new+Pull+Requests
    // Git plugin 3.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/955088898/Triggering+Jenkins+on+new+Pull+Requests+Git+Plugin+3.XX
    pollSCM ''
  } // End triggers
  environment {

    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base"
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
            sh "${ACTIVATE} && nox --session tests -- tests/ -n 3 || true"
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
                        ${ACTIVATE}
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