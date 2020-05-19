// Build variables
def project_name, workspace_shared, package_version, jenkins_env, \
    reqd_tests, lint_vers, python_major_version

// python_major_version is actually the python major version, i.e. 2 or 3,
// for jenkins itself. This is a global variable.
// Within the python common functions py_version is used for the PROJECT python version


// sshagent is only and always used in nodes/ stages, at the highest level

// Common groovy functions

def getPythonDotVersion(py_version) {
    // Pin to a specific dot release of python, so that we use 3.6, not 3.7
    if( py_version == 2) {
        return "2.7"
    } else {
        return "3.6"
    }
}


def createCondaEnv(project_name, py_version=2, py_dot_version=2.7, suffix="") {
    echo "project name: ${project_name}"
    try {sh "conda env remove -y -n ${project_name}_build${py_version}"} catch (err) {}
    if (fileExists("${project_name}${suffix}/conda_environment.yml")) {
        echo 'Found conda_environment.yml. Creating clone...'
        sh "conda env create -f ${project_name}${suffix}/conda_environment.yml -n ${project_name}_build${py_version}"
    } else {
        echo 'No conda environment file. Using *requirements.txt and co.'
        python_dot_version=getPythonDotVersion( py_version )
        // Delete the conda env directory first, it sometimes is left over after a failed build
        // Do it in py2 and py3 minicondas
        sh """
            rm -rf /ihme/code/svcscicompci/miniconda2/envs/${project_name}_build${py_version}
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
                pip install -r requirements.txt --no-cache-dir --extra-index-url=https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple"""
            } catch (err) {
                echo "Caught error while trying to install from requirements, ${err}"
            }
        }
    }
    sh """
        source activate ${project_name}_build${py_version} &> /dev/null
        cd ${project_name}${suffix}
        pip install . --no-cache-dir --extra-index-url=https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
        pip install pytest pytest-cov sphinx sphinx_rtd_theme flake8 flake8-html
        """
}

def addCiCondaEnv(project_name="no_project_name", py_version=2, py_dot_version=2.7) {
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
        rm -rf /ihme/code/svcscicompci/miniconda2/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        rm -rf /ihme/code/svcscicompci/miniconda3/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        git clone ssh://git@stash.ihme.washington.edu:7999/scdo/ci.git /ihme/code/svcscicompci/miniconda${py_version}/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        cd /ihme/code/svcscicompci/miniconda${py_version}/envs/${project_name}_build${py_version}/tmp/ci-tmp${py_version}
        pip install /homes/svcscicompci/deploytools/.
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
    sh "git clone ${repo_url} ${project_name}${suffix}"
    dir("${project_name}${suffix}") {
        sh "git checkout ${branch}"
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

def runRemoteTests(project_name, py_version, workspace_shared, reqd_tests, python_major_version) {

    // Always return 0 for python 3 tests... those shouldn't result in build failures (yet)
    def pytest_cmd
    pytest_cmd = """pytest -m 'not slow' --junitxml=REMOTE-TEST-RESULTS${py_version}.xml"""

    if (!(py_version in reqd_tests)) {
        pytest_cmd = pytest_cmd + " || true"
    }
    sh """
        cd ${project_name}${py_version}
        mv ${project_name} ${project_name}_DO_NOT_USE"""
    try {
        sh """
            cd ${project_name}${py_version}
            source activate ${project_name}_build${py_version} &> /dev/null
            ${pytest_cmd}
        """
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
        sh "conda env remove -y -n ${project_name}_build${py_version}"
        throw e
    }
    junit allowEmptyResults: true, testResults: "**/REMOTE*-RESULTS${py_version}.xml"
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
if ("${skip_tests}".trim().toLowerCase() == "true") { // skipping tests
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

                if ("${python_vers}".trim() == "2") {
                   reqd_tests = [2]
                   lint_vers = 2
                   python_major_version = 2
                } else if ("${python_vers}".trim() == "3") {
                   reqd_tests = [3]
                   lint_vers = 3
                   python_major_version = 3
                } else if ("${python_vers}".trim() == "2 and 3") {
                   reqd_tests = [2, 3]
                   lint_vers = 2
                   python_major_version = 2  // dangerous, cross versions
                } else {
                    throw new Exception("Python version is not set")
                }
            }
        }
        stage('clone repo to build') {
            sshagent (credentials: ['jenkins']) {
                cloneRepoToBuild(project_name, "${python_major_version}")
            }
        }
        stage('create conda env') {
            sshagent (credentials: ['jenkins']) {
                createCondaEnv(project_name, python_major_version, getPythonDotVersion(python_major_version), "${python_major_version}")
                addCiCondaEnv(project_name, python_major_version, getPythonDotVersion(python_major_version))
            }
        }
        stage('publish docs') {
            // It also makes docs hosted here: https://scicomp-docs.ihme.washington.edu/
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
                    mk_symlinks \"${project_name}\" \"/ihme/centralcomp/docs\"
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
                        if ( already_there.startsWith("true" ) ) {
                            echo "Not uploading, ${already_there}"
                        } else {
                            sh """
                                echo "Uploading to ihme-artifactory"
                                source activate ${project_name}_build${python_major_version} &> /dev/null
                                python setup.py sdist
                                twine upload --repository ihme-artifactory ./dist/*
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

                if ("${python_vers}".trim() == "2") {
                   reqd_tests = [2]
                   lint_vers = 2
                   python_major_version = 2
                } else if ("${python_vers}".trim() == "3") {
                   reqd_tests = [3]
                   lint_vers = 3
                   python_major_version = 3
                } else if ("${python_vers}".trim() == "2 and 3") {
                   reqd_tests = [2, 3]
                   lint_vers = 2
                   python_major_version = 2
                } else {
                    throw new Exception("Python version is not set")
                }
            }
        }
    }

    parallel remote_py2: {
        node('qlogin') {
            stage('remote clone repo to build (py2)') {
                sshagent (credentials: ['jenkins']) {
                    cloneRepoToBuild(project_name, "2")
                }
            }
            stage('remote create conda env (py2)') {
                sshagent (credentials: ['jenkins']) {
                    if(2 in reqd_tests){
                        createCondaEnv(project_name, 2, getPythonDotVersion(2), "2")
                        addCiCondaEnv(project_name, 2, getPythonDotVersion(2))
                    } else {
                        echo "Skipping 2 create conda env"
                    }
                }
            }
            stage('run remote tests (py2)') {
                sshagent (credentials: ['jenkins']) {
                    if (lint_vers == 2) {
                        lintProject(project_name, 2)
                    }
                    if(2 in reqd_tests){
                        runRemoteTests(project_name, 2, workspace_shared, reqd_tests, python_major_version)
                    } else {
                        echo "Skipping 2 run tests"
                    }
                }
            }
        }
    }, remote_py3: {
        node('qlogin') {
            stage('remote clone repo to build (py3)') {
                sshagent (credentials: ['jenkins']) {
                    cloneRepoToBuild(project_name, "3")
                }
            }
            stage('remote create conda env (py3)') {
                sshagent (credentials: ['jenkins']) {
                    createCondaEnv(project_name, 3, getPythonDotVersion(3), "3")
                    addCiCondaEnv(project_name, 3, getPythonDotVersion(3))
                }
            }
            stage('run remote tests (py3)') {
                sshagent (credentials: ['jenkins']) {
                    if (lint_vers == 3) {
                        lintProject(project_name, 3)
                    }
                    runRemoteTests(project_name, 3, workspace_shared, reqd_tests, python_major_version)
                }
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
                    // Later: Actually supposedly you can if you escape the dollar-sign with a backslash, as in \$foo
                    already_there = sh(returnStdout: true,
                        script: """
                            source activate ${project_name}_build${python_major_version} &> /dev/null &&
                            is_on_pypi_server ${project_name} ${version_string}""").trim()
                    dir("${project_name}${python_major_version}") {
                        // Do not call setup if there is an existing version with the same number
                        if (already_there.startsWith("true")) {
                            echo "Not uploading, ${already_there}"
                        } else {
                           sh """
                                echo "Uploading to ihme-artifactory"
                                source activate ${project_name}_build${python_major_version} &> /dev/null
                                python setup.py sdist
                                twine upload --repository ihme-artifactory ./dist/*
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