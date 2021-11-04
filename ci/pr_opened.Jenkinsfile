pipeline {
  triggers{
    bitbucketTriggers {
      pullRequestServerCreatedAction()
      pullRequestServerUpdatedAction()
    }
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '30'))
  } // End options
  // triggers {
  //   // This cron expression runs seldom, or never runs, but having the value set
  //   // allows bitbucket server to remotely trigger builds.
  //   // Git plugin 4.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/209059847/Triggering+Jenkins+on+new+Pull+Requests
  //   // Git plugin 3.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/955088898/Triggering+Jenkins+on+new+Pull+Requests+Git+Plugin+3.XX
  //   pollSCM ''
  // } // End triggers
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
        checkout scm: BbS(
          branches: [[name: '${BITBUCKET_SOURCE_BRANCH}']],
          credentialsId: 'svcscicompci',
          extensions: [
            [$class: 'PreBuildMerge',
             options: [mergeRemote: 'jobmon',
                       mergeTarget: '${BITBUCKET_TARGET_BRANCH}']
            ]
          ],
          id: '8370906b-d076-4301-803f-71015eb58456',
          mirrorName: '',
          projectName: 'Scicomp',
          repositoryName: 'jobmon',
          serverId: '54f3b3a3-5d12-4c80-b568-58b59186132d',
          sshCredentialsId: 'jenkins')
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
