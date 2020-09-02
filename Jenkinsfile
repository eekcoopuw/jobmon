// Build variables, global
def project_name, workspace_shared

/* About build variables
Defined inside the Jenkinsfile pipeline:
    project_name: Name of project being built. Derived from the repository URL.
    workspace_shared: The directory where all build-related files are kept by Jenkins.
Defined in the Jenkins configuration page. Jenkinsfile naming convention recommends Jenkins environment variables be shouted in ALL CAPS:
    BRANCH: Which branch of the target repository Jenkins should pull.
    REPO_URL: URL of the target repository.
*/

// sshagent is only and always used in nodes/ stages, at the highest level

// Common groovy functions

def createSetupCfgFile() {
    sh '''
        echo "
        [basic values]
        test_mode=\${test_mode}
        existing_db=\${existing_db}
        same_host=\${same_host}
        db_only=\${db_only}
        use_rsyslog=\${use_rsyslog}
        slack_token=\${slack_token}
        slack_api_url=\${slack_api_url}
        wf_slack_channel=\${wf_slack_channel}
        node_slack_channel=\${node_slack_channel}
        jobmon_server_hostname=\${jobmon_server_hostname}
        jobmon_server_sqdn=\${jobmon_server_sqdn}
        jobmon_service_port=\${jobmon_service_port}
        jobmon_monitor_port=\${jobmon_monitor_port}
        jobmon_version=\${jobmon_version}
        reconciliation_interval=\${reconciliation_interval}
        heartbeat_interval=\${heartbeat_interval}
        report_by_buffer=\${report_by_buffer}
        tag_prefix=\${tag_prefix}
        jobmon_integration_service_port=\${jobmon_integration_service_port}
        loss_threshold=\${loss_threshold}
        poll_interval_minutes=\${poll_interval_minutes}

        [db]
        internal_db_host=\${internal_db_host}
        internal_db_port=\${internal_db_port}
        external_db_host=\${external_db_host}
        external_db_port=\${external_db_port}

        [existing db]
        jobmon_service_user_pwd=\${jobmon_service_user_pwd}

        [same host]
        existing_network=\${existing_network}

        [qpid]
        uri=\${uri}
        cluster=\${cluster}
        pulling_interval=\${pulling_interval}
        max_update_per_second=\${max_update_per_second}

        [rsyslog]
        host=\${rs_host}
        port=\${rs_port}
        protocol=\${rs_protocol}"> /tmp/jobmon.cfg
        '''
}


def cloneRepoToBuild(project_name) {
    sh "echo \$(hostname)"
    sh "rm -rf ${project_name}"
    sh "git clone ${REPO_URL} ${project_name}"
    dir("${project_name}") {
        sh "git checkout ${BRANCH}"
        sh "ls -l setup.cfg"
        createSetupCfgFile()
        sh "cat /tmp/jobmon.cfg"
        sh "cp /tmp/jobmon.cfg jobmon/jobmon.cfg"
        sh "echo setup.cfg"
    }
}


def slackSendFiltered(color, channel, msg) {
    // Utility to send messages to slack channels
    if (color=='bad') {
        // For debugging it is useful to see the environment variables
        sh "printenv"
    }
    if ("${env.JENKINS_IMAGE_VERSION}" == "develop") {
        channel = "ci-integration-test"
    }
    slackSend([color: color,
               channel: channel,
               message: "${JOB_NAME}, JENKINS TEST/NOX build #${env.BUILD_ID}: ${msg}"])
}


def nox_stage(project_name, target, stage_target, nox_sessions, continue_on_failure) {
    // Runs the nox sessions in the nox_sessions string variable
    // ARGS:
    // project_name: is the standard PROJECT_NAME argument from jenkins, e.g. "db_tools"
    // target: The TARGET variable from  the Jenkins pipeline config page,
    //         so it could be "all" or "build" etc
    // stage_target: The specific stage that is being run, so it CAN'T be "all"
    // nox_sesions: A string of the nox session names, blank separated.
    // continue_on_failure: If true, this stage should never report failure, even if it failed
    if (target == "all" || target == "release" || target == stage_target) {
        // A shell trick to ignore failure is to put OR TRUE on the end
        or_true = continue_on_failure ? ' || true' : ''
        try {
            sh """
                echo 'nox ${stage_target} STARTS'
                cd ${project_name}
                nox --session ${nox_sessions} ${or_true}
                echo 'nox ${stage_target} ENDS'
                echo '--------'
            """
        } catch (e) {
            echo 'nox ${stage_target}' + e
            slackSendFiltered('bad', "#gbd-builds-failures", "Failed in nox: ${stage_target}")
            slackSendFiltered('bad', "#gbd-builds-all", "Failed in nox: ${stage_target}")
            throw e
        }
    } else {
        echo "Skipping ${stage_target}"
    }
}


// Pipeline stages
node('qlogin') {
    stage('clone build script & set vars') {
        sshagent (credentials: ['jenkins']) {
            checkout scm

            slackSendFiltered('good', "#gbd-builds-all", "Started")

            // currently only 1 version of base env. may need to have 1 per release of the ci/deploytools repos
            sh """source activate base &> /dev/null"""

            // create workspace for this build
            project_name = "jobmon"
            workspace_shared = "/ihme/scratch/users/svcscicompci/jenkins_workspaces/${project_name}/${env.BUILD_ID}"
            sh "mkdir -p ${workspace_shared}"
            try { sh "chmod -R 777 ${workspace_shared}"} catch(err) { echo "chmod error: ${err}" }
            currentBuild.displayName = "#${BUILD_NUMBER} ${project_name}"
        }
    }
}

node('qlogin') {
    stage('remote clone repo to build') {
        sshagent (credentials: ['jenkins']) {
            cloneRepoToBuild(project_name)
        }
    }
}

// All the NOX stages
node('qlogin') {
    stage('build wheel & docs') {
        sshagent (credentials: ['jenkins']) {
            if (TARGET == "emergency-release" ) {
                // The wheel won't have been built because nox_stage does not check for emergency-release
                nox_stage( project_name, "build", "build", "build docs", false)
            }
            nox_stage( project_name, TARGET, "build", "build docs", false)
        }
    }

    stage('lint & types') {
        sshagent (credentials: ['jenkins']) {
            nox_stage( project_name, TARGET, "lint", "lint typecheck", true)
        }
    }

    stage('tests') {
        sshagent (credentials: ['jenkins']) {
            nox_stage( project_name, TARGET, "tests", "tests", false)
        }
    }

    stage('deploy to pypi and doc servers') {
        sshagent (credentials: ['jenkins']) {
            if (TARGET == "emergency-release" || TARGET == "release") {
                // @TODO This shell fragment could possibly replace versioneer in the publish_docs python script,
                // simplifying the python environment
                nox_stage( project_name, "release", "release", "release", false)

            } else {
                echo "Not a release target, therefore not deploying to artifactory or docs server"
            }
        }
    }

    stage('reports') {
        // TODO Work out how to post test coverage and lint results somewhere
        sshagent (credentials: ['jenkins']) {
            slackSendFiltered('good', "#gbd-builds-all", "COMPLETE")
        }
    }
}
