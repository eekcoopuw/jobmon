pipeline {
  agent none
  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  } // End Options
  parameters {
    string(defaultValue: 'jobmon-dev',
     description: 'Kubernetes Namespace to deploy to',
     name: 'K8S_NAMESPACE')
    string(defaultValue: 'jobmon-dev-ips',
     description: 'Name of the MetalLB IP Pool you wish to get IPs from: https://stash.ihme.washington.edu/projects/ID/repos/metallb-scicomp/browse/k8s/scicomp-cluster-metallb.yml',
     name: 'METALLB_IP_POOL')
    string(defaultValue: 'jobmon-dev-db',
     description: 'name of rancher secret to use for db variables',
     name: 'RANCHER_DB_SECRET')
    string(defaultValue: 'jobmon-slack',
     description: 'name of rancher secret to use for slack variables',
     name: 'RANCHER_SLACK_SECRET')
    string(defaultValue: 'jobmon-qpid',
     description: 'name of rancher secret to use for qpid variables',
     name: 'RANCHER_QPID_SECRET')
    string(defaultValue: 'c-99499:p-4h54h',
     description: 'Rancher project must be created in the rancher web ui before running this job. Get this from the URL after you select the project in the rancher UI. Shouldnt change often',
     name: 'RANCHER_PROJECT_ID')
  }
  triggers {
    // This cron expression runs seldom, or never runs, but having the value set
    // allows bitbucket server to remotely trigger builds.
    // Git plugin 4.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/209059847/Triggering+Jenkins+on+new+Pull+Requests
    // Git plugin 3.x: https://mohamicorp.atlassian.net/wiki/spaces/DOC/pages/955088898/Triggering+Jenkins+on+new+Pull+Requests+Git+Plugin+3.XX
    pollSCM('0 0 1 1 0')
  }
  environment {
    // Jenkins commands run in separate processes, so need to activate the environment to run nox.
    DOCKER_ACTIVATE = ". /mnt/team/scicomp/pub/jenkins/miniconda3/bin/activate base"
    QLOGIN_ACTIVATE = ". /homes/svcscicompci/miniconda3/bin/activate base"
    SCICOMP_DOCKER_REG_URL = "docker-scicomp.artifactory.ihme.washington.edu"
    INFRA_PUB_REG_URL="docker-infrapub.artifactory.ihme.washington.edu"
  }
  stages {
    stage ('Get TARGET_IP address') {
      steps {
        node('docker') {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {
            checkout scm
            // create .jobmon.ini
            sh '''#!/bin/bash
                  . ${WORKSPACE}/ci/deploy_utils.sh
                  get_metallb_cfg "${INFRA_PUB_REG_URL}/kubectl:latest" ${WORKSPACE}
               '''
          }
          script {
            // TODO: more robust parsing script
            env.TARGET_IP = sh (
              script: '''#!/bin/bash
                         . ${WORKSPACE}/ci/deploy_utils.sh
                         get_metallb_ip "${METALLB_IP_POOL}" ${WORKSPACE}
                      ''',
              returnStdout: true
            ).trim()
          }
          echo "Setting TARGET_IP=${env.TARGET_IP}"
        }
      }
    }
    stage ('Build Python Distribution') {
      steps {
        node('docker') {
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'REG_USERNAME',
                                            passwordVariable: 'REG_PASSWORD')]) {
            sh '''#!/bin/bash
              INI=${WORKSPACE}/jobmon/.jobmon.ini
              rm $INI
              echo "[client]\nweb_service_fqdn=${TARGET_IP}\nweb_service_port=80" > $INI
              ${DOCKER_ACTIVATE} && nox --session distribute
              PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
              twine upload --repository-url $PYPI_URL \
                           --username $REG_USERNAME \
                           --password $REG_PASSWORD \
                           ./dist/*
            '''
            script {
              env.JOBMON_VERSION = sh (
                script: '''
                basename $(find ./dist/jobmon-*.tar.gz) | sed "s/jobmon-\\(.*\\)\\.tar\\.gz/\\1/"
                ''',
                returnStdout: true
              ).trim()
            }
            echo "Jobmon Version=${env.JOBMON_VERSION}"
          }
        }
      }
    }
    stage ('Build Server Containers') {
      steps {
        node('docker') {
          script {
            env.JOBMON_CONTAINER_URI = sh (
              script: '''#!/bin/bash
                # check if dev is in the version string and pick a container name based on that
                if [[ "$JOBMON_VERSION" =~ "dev" ]]
                then
                  CONTAINER_NAME="jobmon_dev"
                else
                  CONTAINER_NAME="jobmon"
                fi
                echo "${SCICOMP_DOCKER_REG_URL}/$CONTAINER_NAME:${JOBMON_VERSION}"
              ''',
              returnStdout: true
            ).trim()
            env.GRAFANA_CONTAINER_URI = sh (
              script: '''echo ${SCICOMP_DOCKER_REG_URL}/${K8S_NAMESPACE}-grafana:${BUILD_NUMBER}''',
              returnStdout: true
            ).trim()
          }
          echo "Server Container Images:\nJobmon=${env.JOBMON_CONTAINER_URI}\nGrafana=${env.GRAFANA_CONTAINER_URI}"
          // Artifactory user with write permissions
          withCredentials([usernamePassword(credentialsId: 'artifactory-docker-scicomp',
                                            usernameVariable: 'REG_USERNAME',
                                            passwordVariable: 'REG_PASSWORD')]) {
            // this builds a requirements.txt with the correct jobmon version number and uses
            // it to build a dockerfile for the jobmon services
            sh '''#!/bin/bash
              # build jobmon container
              echo "jobmon==${JOBMON_VERSION}" > ${WORKSPACE}/requirements.txt
              docker login -u "$REG_USERNAME" -p "$REG_PASSWORD" "https://${SCICOMP_DOCKER_REG_URL}"
              docker build --no-cache -t "${JOBMON_CONTAINER_URI}" -f ./deployment/k8s/Dockerfile .
              docker push "${JOBMON_CONTAINER_URI}"

              # build grafana container
              docker build --no-cache -t "${GRAFANA_CONTAINER_URI}" -f ./deployment/k8s/Dockerfile .
              docker push "${GRAFANA_CONTAINER_URI}"
            '''
          }
        }
      }
    }
    stage ('Deploy K8s') {
      steps {
        node('docker') {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {

            sh '''#!/bin/bash
              # Render each .yaml.j2 template in the k8s dir (return only the basename)
              YASHA_CONTAINER="${INFRA_PUB_REG_URL}/yasha:latest"
              docker pull $YASHA_CONTAINER
              for TEMPLATE in $(find "${WORKSPACE}/deployment/k8s/" -maxdepth 1 -type f -name '*.yaml.j2' -printf "%f\n"|sort -n)
              do
                docker run \
                --rm \
                -v "${WORKSPACE}/deployment/k8s:/data" \
                -t \
                $YASHA_CONTAINER \
                --jobmon_container_uri="${JOBMON_CONTAINER_URI}" \
                --ip_pool="${METALLB_IP_POOL}" \
                --namespace="${K8S_NAMESPACE}" \
                --rancherproject="${RANCHER_PROJECT_ID}" \
                --grafana_image="${GRAFANA_CONTAINER_URI}" \
                --rancher_db_secret="${RANCHER_DB_SECRET}" \
                --rancher_slack_secret="${RANCHER_SLACK_SECRET}" \
                --rancher_qpid_secret="${RANCHER_QPID_SECRET}" \
                /data/${TEMPLATE}
              done

              chown -R "$(id -u):$(id -g)" .

              KUBECTL_CONTAINER="${INFRA_PUB_REG_URL}/kubectl:latest"
              docker pull $KUBECTL_CONTAINER
              docker run -t \
                --rm \
                -v ${KUBECONFIG}:/root/.kube/config \
                -v "${WORKSPACE}/deployment/k8s:/data" \
                ${KUBECTL_CONTAINER} \
                get namespace "${K8S_NAMESPACE}" ||
                docker run -t \
                  --rm \
                  -v ${KUBECONFIG}:/root/.kube/config \
                  -v "${WORKSPACE}/deployment/k8s:/data" \
                  ${KUBECTL_CONTAINER} \
                  apply \
                  -f /data/01_namespace.yaml
              # Remove the rendered namespace setup yaml before proceeding
              rm -f ./deployment/k8s/01_namespace.yaml

              log_info Deploying deployment and service configurations to k8s
              for TEMPLATE in $(find "${WORKSPACE}/deployment/k8s/" -maxdepth 1 -type f -name '*.yaml' -printf "%f\n" |sort -n)
              do
                docker run -t \
                --rm \
                -v ${KUBECONFIG}:/root/.kube/config \
                -v "${WORKSPACE}/deployment/k8s:/data" \
                ${KUBECTL_CONTAINER} \
                --namespace="${K8S_NAMESPACE}" \
                apply \
                -f /data/${TEMPLATE}
              done
            '''
          }
        }
      }
    }
    stage ('Test Deployment') {
      steps {
        node('qlogin') {
          checkout scm
          sh '''#!/bin/bash
            CONDA_DIR=${WORKSPACE}/.conda_env/load_test
            ${QLOGIN_ACTIVATE} && conda create --prefix $CONDA_DIR python==3.7
            ${QLOGIN_ACTIVATE} && conda activate $CONDA_DIR && \
              pip install jobmon==${JOBMON_VERSION} && \
              python ${WORKSPACE}/deployment/tests/six_job_test.py
          '''
        }
      }
    }
  }
  post {
    always {
      node('docker') {
        // Delete the workspace directory.
        deleteDir()
      }
      node('qlogin') {
        // Delete the workspace directory.
        deleteDir()
      }
    }
  }
}
