// Build variables
def project_name, workspace_shared, jenkins_env, lint_vers, python_major_version

// python_major_version is actually the python major version, i.e. 2 or 3,
// for jenkins itself. This is a global variable.
// Within the python common functions py_version is used for the PROJECT python version


// sshagent is only and always used in nodes/ stages, at the highest level


def createCondaEnv(project_name, py_version=3, py_dot_version=3.6, suffix="") {
    echo "project name: ${project_name}"
    try {sh "conda env remove -y -n ${project_name}_build${py_version}"} catch (err) {}
    if (fileExists("${project_name}${suffix}/conda_environment.yml")) {
        echo 'Found conda_environment.yml. Creating clone...'
        sh "conda env create -f ${project_name}${suffix}/conda_environment.yml -n ${project_name}_build${py_version}"
    } else {
        echo 'No conda environment file. Using *requirements.txt and co.'
        // Delete the conda env directory first, it sometimes is left over after a failed build
        sh """
            rm -rf /ihme/code/svcscicompci/miniconda3/envs/${project_name}_build${py_version}
            conda create -n ${project_name}_build${py_version} python=${py_dot_version}
            source activate ${project_name}_build${py_version} &> /dev/null
            wget https://bootstrap.pypa.io/ez_setup.py -O - | python
            pip install cython
        """
        // Trying different methods in a sequence, hence we hide the errors
        if (fileExists("${project_name}${suffix}/conda_requirements.txt")) {
            echo 'Found conda_requirements.txt, installing from file'
            try {sh """
                source activate ${project_name}_build${py_version} &> /dev/null
                cd ${project_name}${suffix}
                conda install --file conda_requirements.txt -y
                conda install -c conda-forge -y openssl
                """
            } catch (err) {
                echo "Caught error while trying to install from conda_requirements, ${err}"
            }
        }
        if (fileExists("${project_name}${suffix}/requirements.txt")) {
            echo 'Found requirements.txt, installing from file'
            try {sh """
                source activate ${project_name}_build${py_version} &> /dev/null
                cd ${project_name}${suffix}
                pip install -r requirements.txt --no-cache-dir --extra-index-url=http://pypi.services.ihme.washington.edu/simple --trusted-host=pypi.services.ihme.washington.edu"""
            } catch (err) {
                echo "Caught error while trying to install from requirements, ${err}"
            }
        }
    }
    sh """
        source activate ${project_name}_build${py_version} &> /dev/null
        cd ${project_name}${suffix}
        pip install . --no-cache-dir --extra-index-url=http://pypi.services.ihme.washington.edu/simple --trusted-host=pypi.services.ihme.washington.edu
        pip install pytest pytest-cov sphinx sphinx_rtd_theme flake8 flake8-html
        """
}

def addCiCondaEnv(project_name="no_project_name", py_version=3, py_dot_version=3.6) {
    // This is a common conda environment that contains non-project-specific
    // build tools.
    // Choices:
    // 1. Put it into each project environments. Risk of version conflicts.
    // 2. Have a static one that we build by hand when jenkins is updated without Docker. Hard to maintain
    // 3. Have a shared one that could possibly be updated by any build -> race condition. Bad

    // Go with 1. In the docker version it was number 2

    // Remember that deploytools are down in a subdirectory
    // Sometimes there are p2 envs in both miniconda2 and miniconda3. The real version is in python_major_version

    sh """
        source activate ${project_name}_build${py_version} &> /dev/null
        rm -rf /ihme/code/svcscicompci/miniconda3/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        git clone ssh://git@stash.ihme.washington.edu:7999/scdo/ci.git /ihme/code/svcscicompci/miniconda${py_version}/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        cd /ihme/code/svcscicompci/miniconda${py_version}/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        pip install /ihme/homes/svcscicompci/deploytools/.
    """
}

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

        [db]
        internal_db_host=\${internal_db_host}
        internal_db_port=\${internal_db_port}
        external_db_host=\${external_db_host}
        external_db_port=\${external_db_port}

        [existing db]
        jobmon_service_user_pwd=\${jobmon_service_user_pwd}

        [same host]
        existing_network=\${existing_network}


        [rsyslog]
        host=\${rs_host}
        port=\${rs_port}
        protocol=\${rs_protocol}"> /tmp/jobmon.cfg
        '''
}

def cloneRepoToBuild(project_name, suffix="") {
    sh "echo \$(hostname)"
    sh "rm -rf ${project_name}${suffix}"
    sh "git clone ${repo_url} --branch ${branch}  --single-branch ${project_name}${suffix}"
    sh "whoami"
    dir("${project_name}${suffix}") {
        sh "ls -l setup.cfg"
        createSetupCfgFile()
        sh "cat /tmp/jobmon.cfg"
        sh "cp /tmp/jobmon.cfg jobmon/jobmon.cfg"
        sh "echo setup.cfg"
    }
}

def lintProject(project_name, lint_vers) {
    try {
        sh """
            cd ${project_name}${lint_vers}
            source activate ${project_name}_build${lint_vers} &> /dev/null
            flake8 --format=html --htmldir=${env.WORKSPACE}/flake-report ${project_name}
        """
    } catch(err) {}
    publishHTML (target: [
      allowMissing: false,
      alwaysLinkToLastBuild: false,
      keepAll: true,
      reportDir: "${env.WORKSPACE}/flake-report",
      reportFiles: 'index.html',
      reportName: "Flake8 report"
    ])
}

def runRemoteTests(project_name, py_version, workspace_shared, python_major_version) {

    def pytest_cmd
    pytest_cmd = """
        pytest --cov=${project_name} --cov-append \
        --cov-report html:${env.WORKSPACE}/cov_html_remote${py_version} \
        -m 'not slow' --junitxml=REMOTE-TEST-RESULTS${py_version}.xml"""

    sh """
        cd ${project_name}${py_version}
        mv ${project_name} ${project_name}_DO_NOT_USE"""
    try {
        withEnv(["COVERAGE_FILE=${workspace_shared}/coverage${py_version}-remote.dat"]) {
            sh """
                cd ${project_name}${py_version}
                source activate ${project_name}_build${py_version} &> /dev/null
                ${pytest_cmd}
            """
        }
        if (py_version == python_major_version) {
            // Restore original name of package directory so it can be uploaded
            sh """
                cd ${project_name}${py_version}
                mv ${project_name}_DO_NOT_USE ${project_name}
                source activate ${project_name}_build${py_version} &> /dev/null
                make html
            """
        }
        version_string = sh(returnStdout: true,
            script: """
                source activate ${project_name}_build${py_version} &> /dev/null
                pip freeze | sed -e 's/-/_/g' | grep ${project_name} | sed -e 's/^.*==//'
            """
        ).trim()
    } catch(e) {
        echo 'Remote tests failed'
        slackSendFiltered('bad', "#gbd-builds-failures", "Failed")
        slackSendFiltered('bad', "#gbd-builds-all", "Failed")
        junit "**/REMOTE*-RESULTS${py_version}.xml"
        publishHTML (target: [
          allowMissing: true,
          alwaysLinkToLastBuild: false,
          keepAll: true,
          reportDir: "${env.WORKSPACE}/cov_html_remote${py_version}",
          reportFiles: 'index.html',
          reportName: "Remote test coverage (py${py_version})"
        ])
        sh "conda env remove -y -n ${project_name}_build${py_version}"
        throw e
    }
    junit allowEmptyResults: true, testResults: "**/REMOTE*-RESULTS${py_version}.xml"
    publishHTML (target: [
      allowMissing: true,
      alwaysLinkToLastBuild: false,
      keepAll: true,
      reportDir: "${env.WORKSPACE}/cov_html_remote${py_version}",
      reportFiles: 'index.html',
      reportName: "Remote test coverage (py${py_version})"
    ])
}

def slackSendFiltered(color, channel, msg) {
    if (color=='bad') {
        sh "printenv"
    }
    if ("${env.JENKINS_IMAGE_VERSION}" == "develop") {
        channel = "ci-integration-test"
    }
    slackSend([color: color,
               channel: channel,
               message: "${JOB_NAME}, build #${env.BUILD_ID}: ${msg}"])
}



// Pipeline stages

// Create new setup.cfg based on Jenkins parameters and save them in a string

if ("${skip_tests}".trim().toLowerCase() == "true") { // skipping tests
    node('qlogin') {
        stage('clone build script & set vars') {
            sshagent (credentials: ['jenkins']) {
                checkout scm

                slackSendFiltered('good', "#gbd-builds-all", "Started")
                project_name = "jobmon"
                workspace_shared = "/ihme/scratch/users/svcscicompci/jenkins_workspaces/${project_name}/${env.BUILD_ID}"
                sh "mkdir -p ${workspace_shared}"
                try { sh "chmod -R 777 ${workspace_shared}"} catch(err) { echo "${err}" }
                currentBuild.displayName = "#${BUILD_NUMBER} ${project_name}"

                python_major_version = 3
                python_dot_version = 3.6
            }
        }
        stage('clone repo to build') {
            sshagent (credentials: ['jenkins']) {
                cloneRepoToBuild(project_name, "${python_major_version}")
            }
        }
        stage('create conda env') {
            sshagent (credentials: ['jenkins']) {
                createCondaEnv(project_name, python_major_version, python_dot_version, "${python_major_version}")
                addCiCondaEnv(project_name, python_major_version, python_dot_version)
            }
        }
        stage('publish docs') {
            // It also makes docs
            sshagent (credentials: ['jenkins']) {
                sh """
                    cd ${project_name}${python_major_version}
                    source activate ${project_name}_build${python_major_version} &> /dev/null
                    make html
                """
                version_string = sh(returnStdout: true,
                    script: """
                        source activate ${project_name}_build${python_major_version} &> /dev/null &&
                        pip freeze | sed -e 's/-/_/g' | grep ${project_name} | sed -e 's/^.*==//'
                    """
                    ).trim()
                sh """
                    source activate ${project_name}_build${python_major_version} &> /dev/null
                    publish_docs \"${project_name}\" \"${env.WORKSPACE}/${project_name}\" \"/ihme/centralcomp/docs\"
                """
                sh """
                    source activate ${project_name}_build${python_major_version} &> /dev/null
                    
                """
            }
        }
        stage('deploy to pypi server') {
            sshagent (credentials: ['jenkins']) {
                try {
                    // Sadness. Cannot use variables defined inside a shell script, so run the script and assign to a Groovy var
                    // Deeper sadness. I wrote a python script to call the versioneer to get the version, but it did not work.
                    // Forced to write this hacky piece of shell.
                    // Later: Actually you can do it, but must escape the $-sign when you use it so that the groovy compiler
                    // does not look for a groovy variable. e.g. \$var
                    already_there = sh(returnStdout: true,
                        script: """
                            source activate ${project_name}_build${python_major_version} &> /dev/null &&
                            is_on_pypi_server ${project_name} ${version_string}
                        """).trim()
                    dir("${project_name}${python_major_version}") {
                        // Do not call setup if there is an existing version with the same number
                        if ( already_there.startsWith("true" ) && "${overide_package_on_pypi}".toBoolean() == false ) {
                            echo "Not uploading, ${already_there}"
                        } else {
                            sh """
                                hostname
                                source activate ${project_name}_build${python_major_version} &> /dev/null
                                python setup.py sdist
                                mv ./dist/* /ihme/pypi/
                            """
                        }
                    }
                    sh "conda env remove -y -n ${project_name}_build${python_major_version}"
                } catch (e) {
                    echo "Deploy failed with message: ${e}"
                    slackSendFiltered('bad', "#gbd-builds-failures", "Failed in deploy")
                    slackSendFiltered('bad', "#gbd-builds-all", "Failed in deploy")
                    throw e
                }
            }
        }
    }
} else { // Not skipping tests
    node('qlogin') {
        stage('clone build script & set vars') {
            sshagent (credentials: ['jenkins']) {
                checkout scm

                slackSendFiltered('good', "#gbd-builds-all", "Started")
                project_name = sh(returnStdout: true,
                                  script: "bin/proj_name_from_repo ${repo_url}").trim()
                workspace_shared = "/ihme/scratch/users/svcscicompci/jenkins_workspaces/${project_name}/${env.BUILD_ID}"
                sh "mkdir -p ${workspace_shared}"
                try { sh "chmod -R 777 ${workspace_shared}"} catch(err) { echo "${err}" }
                currentBuild.displayName = "#${BUILD_NUMBER} ${project_name}"

                lint_vers = 3
                python_major_version = 3
                python_dot_version = 3.6
            }
        }
    }

    node('qlogin') {
        stage('remote clone repo to build (py3)') {
            sshagent (credentials: ['jenkins']) {
                cloneRepoToBuild(project_name, "${python_major_version}")
            }
        }
        stage('remote create conda env (py3)') {
            sshagent (credentials: ['jenkins']) {
                createCondaEnv(project_name, python_major_version, python_dot_version, "${python_major_version}")
                addCiCondaEnv(project_name, python_major_version, python_dot_version)
            }
        }
        stage('run remote tests (py3)') {
            sshagent (credentials: ['jenkins']) {
                lintProject(project_name, 3)
                runRemoteTests(project_name, 3, workspace_shared, python_major_version)
            }
        }
    }

    node('qlogin') {
        stage('make docs') {
            sshagent (credentials: ['jenkins']) {
                try {
                    // @2 in the path is a hack, i think it is the second branch of the parallel test run (py3) above
                    // runRemoteTests also builds the html, could be in either version of python
                    // ${python_major_version}
                    sh """
                        source activate ${project_name}_build${python_major_version} &> /dev/null &&
                        publish_docs \"${project_name}\" \"${env.WORKSPACE}/${project_name}\" \"/ihme/centralcomp/docs\"
                    """
                    sh """
                        source activate ${project_name}_build${python_major_version} &> /dev/null &&
                        mk_symlinks \"${project_name}\" \"/ihme/centralcomp/docs\"
                    """
                } catch (e) {
                    echo 'make-docs failed'
                    slackSendFiltered('bad', "#gbd-builds-failures", "Failed in make-docs")
                    slackSendFiltered('bad', "#gbd-builds-all", "Failed in make-docs")
                    throw e
                }
            }
        }

        stage('deploy to pypi server') {
            sshagent (credentials: ['jenkins']) {
                try {
                    // Sadness. Cannot use variables defined inside a shell script, so run the script and assign to a Groovy var
                    // Deeper sadness. I wrote a python script to call the versioneer to get the version, but it did not work.
                    // Forced to write this hacky piece of shell.
                    // Later: Actually supposedly you can if you escape the dollar-sogn with a backslash, as in \$foo
                    already_there = sh(returnStdout: true,
                        script: """
                            hostname
                            source activate ${project_name}_build${python_major_version} &> /dev/null &&
                            is_on_pypi_server ${project_name} ${version_string}""").trim()
                    dir("${project_name}${python_major_version}") {
                        // Do not call setup if there is an existing version with the same number
                        if ( already_there.startsWith("true" ) && "${overide_package_on_pypi}".toBoolean() == false ) {
                            echo "Not uploading, ${already_there}"
                        } else {
                           sh """
                                source activate ${project_name}_build${python_major_version} &> /dev/null
                                twine upload --repository ihme-artifactory dist/*
                            """
                        }
                    }
                } catch (e) {
                    echo "Deploy failed with message: ${e}"
                    slackSendFiltered('bad', "#gbd-builds-failures", "Failed in deploy")
                    slackSendFiltered('bad', "#gbd-builds-all", "Failed in deploy")
                    throw e
                }
            }
        }
    }
}
