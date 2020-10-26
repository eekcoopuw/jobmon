pipeline {
  agent { label 'docker' }
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '50'))
  } // End options
  parameters {
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
    string(defaultValue: 'jobmon-dev-ips',
     description: 'Name of the MetalLB IP Pool you wish to get IPs from: https://stash.ihme.washington.edu/projects/ID/repos/metallb-scicomp/browse/k8s/scicomp-cluster-metallb.yml',
     name: 'METALLB_IP_POOL')
    string(defaultValue: 'docker-scicomp.artifactory.ihme.washington.edu/jobmon:guppy_7_20200611_103157',
     description: 'Jobmon Container URI <registry>/<container>:<tag>',
     name: 'JOBMON_CONTAINER_URI')
    string(defaultValue: 'scicomp-maria-db-d01.db.ihme.washington.edu',
     description: 'Database host name that the jobmon database is located in',
     name: 'DB_HOST')
    string(defaultValue: '3306',
     description: 'Database host port number',
     name: 'DB_PORT')
    string(defaultValue: 'c-99499:p-4h54h',
     description: 'Rancher project must be created in the rancher web ui before running this job. Get this from the URL after you select the project in the rancher UI. Shouldnt change often',
     name: 'RANCHER_PROJECT_ID')
    string(defaultValue: 'jobmon-alerts',
     description: 'Workflow Slack Channel (default: jobmon-alerts)',
     name: 'WF_SLACK_CHANNEL')
  } // End parameters
  stages {
    stage('build') {
      steps {
        node('docker') {
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'DOCKER_REG_USERNAME',
                                            passwordVariable: 'DOCKER_REG_PASSWORD')]) {
            // Scicomp kubernetes cluster container
            withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                  variable: 'KUBECONFIG')]) {
              // Jobmon Database Credentials
              withCredentials([usernamePassword(credentialsId: 'jobmon-dev-db-userpass',
                                                usernameVariable: 'DB_USER',
                                                passwordVariable: 'DB_PASS')]) {
                // Slack token for sending slack messages
                withCredentials([string(credentialsId: 'jobmon-slack-token',
                                        variable: 'SLACK_TOKEN')]) {
                  wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'xterm']) {
                    checkout scm
                    sh '''
                    # PWD when job starts will be root of ${WORKSPACE}
                    # Source common functions
                    . ${WORKSPACE}/common.sh || (echo Failed to import common.sh; exit 1)

                    # Generate a URI for the grafana container
                    export GRAFANA_CONTAINER_URI=${SCICOMP_REG_URL}/${K8S_NAMESPACE}-grafana:${BUILD_NUMBER}

                    # Fix permissions and cleanup
                    delete_files_not_in_git

                    # Pull docker images before we use them
                    pull_common_container_images

                    build_push_container ${SCICOMP_REG_URL} \
                       "${DOCKER_REG_USERNAME}" \
                       "${DOCKER_REG_PASSWORD}" \
                       "${GRAFANA_CONTAINER_URI}" \
                       k8s/grafana/Dockerfile

                    log_info Rendering deployment templates
                    # Render each .yaml.j2 template in the k8s dir (return only the basename)
                    for TEMPLATE in $(find "${WORKSPACE}/k8s/" -maxdepth 1 -type f -name '*.yaml.j2' -printf "%f\n"|sort -n)
                    do
                      docker run \
                      --rm \
                      -v "${WORKSPACE}/k8s:/data" \
                      -t \
                      ${YASHA_CONTAINER} \
                      --jobmon_container_uri="${JOBMON_CONTAINER_URI}" \
                      --ip_pool="${METALLB_IP_POOL}" \
                      --namespace="${K8S_NAMESPACE}" \
                      --db_pass="${DB_PASS}" \
                      --db_user="${DB_USER}" \
                      --db_host="${DB_HOST}" \
                      --db_port="${DB_PORT}" \
                      --jobmon_port="80" \
                      --slack_token="${SLACK_TOKEN}" \
                      --wf_slack_channel="${WF_SLACK_CHANNEL}" \
                      --rancherproject="${RANCHER_PROJECT_ID}" \
                      --grafana_image="${GRAFANA_CONTAINER_URI}" \
                      /data/${TEMPLATE}
                    done

                    fix_ownership

                    log_info Checking if the namespace ${K8S_NAMESPACE} exists. Creating it if it doesnt
                    docker run -t \
                      --rm \
                      -v ${KUBECONFIG}:/root/.kube/config \
                      -v "${WORKSPACE}/k8s:/data" \
                      ${KUBECTL_CONTAINER} \
                      get namespace "${K8S_NAMESPACE}" ||
                      docker run -t \
                        --rm \
                        -v ${KUBECONFIG}:/root/.kube/config \
                        -v "${WORKSPACE}/k8s:/data" \
                        ${KUBECTL_CONTAINER} \
                        apply \
                        -f /data/01_namespace.yaml
                    # Remove the rendered namespace setup yaml before proceeding
                    rm -f k8s/01_namespace.yaml

                    log_info Deploying deployment and service configurations to k8s
                    for TEMPLATE in $(find "${WORKSPACE}/k8s/" -maxdepth 1 -type f -name '*.yaml' -printf "%f\n" |sort -n)
                    do
                      docker run -t \
                      --rm \
                      -v ${KUBECONFIG}:/root/.kube/config \
                      -v "${WORKSPACE}/k8s:/data" \
                      ${KUBECTL_CONTAINER} \
                      --namespace="${K8S_NAMESPACE}" \
                      apply \
                      -f /data/${TEMPLATE}
                    done

                    delete_files_not_in_git
                    '''
                  } // End Wrap
                } // End Slack Token
              } // End DB Credentials
            } // End KUBECONFIG Credentials
          } // End Artifactory Credentials
        } // End Node Docker
      } // End Steps
    } // End Stage
  } // End Stages
}  // End Pipeline
