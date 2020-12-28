pipeline {
  agent None
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
    string(defaultValue: 'foo',
     description: 'name of configmap to use for server variables',
     name: 'K8S_CONFIGMAP')
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
    ACTIVATE = "source /homes/svcscicompci/miniconda3/bin/activate base &> /dev/null"
  }
  stages {
    stage ('Write "jobmon.ini" File') {
      steps {
        node('docker') {
          // Scicomp kubernetes cluster container
          withCredentials([file(credentialsId: 'k8s-scicomp-cluster-kubeconf',
                                variable: 'KUBECONFIG')]) {
            sh '''
            # get metallb configmap
            kubectl -n metallb-system \
              get configmap config -o "jsonpath={.data.config}" | \
              grep -A 4 ${METALLB_IP_POOL} > metallb_ip.cfg

            TARGET_IP=$(cat metallb_ip.cfg | \
                        grep "\- [0-9].*/[0-9]*" | \
                        sed -e "s/  - \(.*\)\/32/\1/")
            echo "
            [client]
            web_service_fqdn=$TARGET_IP
            web_service_port=80
            " > .jobmon.ini
            '''
          }
        }
      }
    }
  }
}
