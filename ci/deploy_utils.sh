export HELM_CONTAINER="docker.artifactory.ihme.washington.edu/alpine/helm:latest"
export KUBECTL_CONTAINER="docker-infrapub.artifactory.ihme.washington.edu/kubectl:latest"

get_metallb_cfg () {
    WORKSPACE=$1

    # pull kubectl container
    docker pull $KUBECTL_CONTAINER

    # get metallb configmap
    docker run \
        -t \
        --rm \
        -v ${KUBECONFIG}:/root/.kube/config \
        --mount type=bind,source="$WORKSPACE",target=/data \
        $KUBECTL_CONTAINER  \
            -n metallb-system \
            get configmap config \
            -o "jsonpath={.data.config}" > $WORKSPACE/metallb.cfg
}


get_metallb_ip_from_cfg () {
    METALLB_IP_POOL=$1
    WORKSPACE=$2

    # 4th line after entry is VIP.
    grep -A 4 "$METALLB_IP_POOL" $WORKSPACE/metallb.cfg | \
    grep "\- [0-9].*/[0-9]*" | \
    sed -e "s/  - \(.*\)\/32/\1/"
}


upload_python_dist () {
    WORKSPACE=$1
    REG_USERNAME=$2
    REG_PASSWORD=$3
    ACTIVATE=$4

    $ACTIVATE && nox --session distribute
    PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
    JOBMON_VERSION=$(basename $(find ./dist/jobmon-*.tar.gz) | sed "s/jobmon-\\(.*\\)\\.tar\\.gz/\\1/")
    if [[ "$JOBMON_VERSION" =~ "dev" ]]
    then
      $ACTIVATE && twine upload \
        --repository-url $PYPI_URL \
        --username $REG_USERNAME \
        --password $REG_PASSWORD \
        --skip-existing \
        ./dist/*
    else
      $ACTIVATE && twine upload \
        --repository-url $PYPI_URL \
        --username $REG_USERNAME \
        --password $REG_PASSWORD \
        ./dist/*
    fi
    echo "Jobmon v$JOBMON_VERSION deployed to Pypi"

}


get_container_name_from_version () {
    JOBMON_VERSION=$1
    SCICOMP_DOCKER_REG_URL=$2

    # check if dev is in the version string and pick a container name based on that
    if [[ "$JOBMON_VERSION" =~ "dev" ]]
    then
      CONTAINER_NAME="jobmon_dev"
    else
      CONTAINER_NAME="jobmon"
    fi
    echo "$SCICOMP_DOCKER_REG_URL/$CONTAINER_NAME:$JOBMON_VERSION"
}


upload_jobmon_image () {
    JOBMON_VERSION=$1
    WORKSPACE=$2
    REG_USERNAME=$3
    REG_PASSWORD=$4
    SCICOMP_DOCKER_REG_URL=$5
    JOBMON_CONTAINER_URI=$6
    GRAFANA_CONTAINER_URI=$7


    # build jobmon container
    echo "jobmon[server]==$JOBMON_VERSION" > $WORKSPACE/requirements.txt
    docker login -u "$REG_USERNAME" -p "$REG_PASSWORD" "https://$SCICOMP_DOCKER_REG_URL"
    docker build --no-cache -t "$JOBMON_CONTAINER_URI" -f ./deployment/k8s/Dockerfile .
    docker push "$JOBMON_CONTAINER_URI"

    # build grafana container
    docker build --no-cache -t "$GRAFANA_CONTAINER_URI" -f ./deployment/k8s/grafana/Dockerfile .
    docker push "$GRAFANA_CONTAINER_URI"

}


deploy_jobmon_to_k8s () {
    WORKSPACE=${1}
    JOBMON_CONTAINER_URI=${2}
    METALLB_IP_POOL=${3}
    K8S_NAMESPACE=${4}
    RANCHER_PROJECT_ID=${5}
    GRAFANA_CONTAINER_URI=${6}
    RANCHER_DB_SECRET=${7}
    RANCHER_SLACK_SECRET=${8}
    RANCHER_QPID_SECRET=${9}
    KUBECONFIG=${10}
    USE_LOGSTASH=${11}
    JOBMON_VERSION=${12}
    K8S_REAPER_NAMESPACE=${13}
    DEPLOY_JOBMON=${14}
    DEPLOY_ELK=${15}

    docker pull $HELM_CONTAINER  # Pull prebuilt helm container
    docker pull $KUBECTL_CONTAINER

    # Check if namespace exists, if not create it: render 01_namespace.yaml and apply it
    docker run -t \
        --rm \
        -v $KUBECONFIG:/root/.kube/config \
        -v "$WORKSPACE/deployment/k8s:/data" \
        $KUBECTL_CONTAINER \
            get namespace "$K8S_NAMESPACE"\

    namespace_exists=$?
    if [[ $namespace_exists -ne 0 ]]
    then
        echo "Namespace does not exist now creating it "
        docker run -t \
            --rm \
            -v $KUBECONFIG:/root/.kube/config \
            -v "$WORKSPACE/deployment/k8s/jobmon:/data" \
            alpine/helm \
                template /data -s templates/01_namespace.yaml \
                --set global.namespace="$K8S_NAMESPACE" \
                --set global.rancher_project="$RANCHER_PROJECT_ID" >> \
                "$WORKSPACE/deployment/k8s/jobmon/namespace.yaml"
        docker run -t \
            --rm \
            -v "$WORKSPACE/deployment/k8s/jobmon:/data" \
            -v $KUBECONFIG:/root/.kube/config \
            ${KUBECTL_CONTAINER} apply -f /data/namespace.yaml
    fi

    # Remove file, so helm doesn't attempt to re-deploy it
    rm -f ./deployment/k8s/jobmon/templates/01_namespace.yaml

    if [[ "$DEPLOY_ELK" = true ]]
    then
        echo "Creating or updating Jobmon-ELK deployment"
        docker run -t \
        --rm \
        -v "$WORKSPACE/deployment/k8s/elk:/apps" \
        -v $KUBECONFIG:/root/.kube/config \
        alpine/helm \
            upgrade --install jobmon-elk /apps/. \
            -n "$K8S_NAMESPACE" \
            --set global.namespace="$K8S_NAMESPACE" \
            --set metricbeat.db_host_secret="$RANCHER_DB_SECRET"
    fi

    if [[ "$DEPLOY_JOBMON" = true ]]
    then
        echo "Creating or updating Jobmon deployment"
        docker run -t \
        --rm \
        -v "$WORKSPACE/deployment/k8s/jobmon:/apps" \
        -v $KUBECONFIG:/root/.kube/config \
        alpine/helm \
            upgrade --install jobmon /apps/. \
            -n "$K8S_NAMESPACE" \
            --set global.grafana_image="$GRAFANA_CONTAINER_URI" \
            --set global.jobmon_container_uri="$JOBMON_CONTAINER_URI" \
            --set global.metallb_ip_pool="$METALLB_IP_POOL" \
            --set global.namespace="$K8S_NAMESPACE" \
            --set global.rancher_db_secret="$RANCHER_DB_SECRET" \
            --set global.rancher_project="$RANCHER_PROJECT_ID" \
            --set global.rancher_qpid_secret="$RANCHER_QPID_SECRET" \
            --set global.rancher_slack_secret="$RANCHER_SLACK_SECRET" \
            --set global.use_logstash="$USE_LOGSTASH"

        echo "Adding new reaper to reapers namespace"
        docker run -t \
        --rm \
        -v "$WORKSPACE/deployment/k8s/reapers:/apps" \
        -v $KUBECONFIG:/root/.kube/config \
        alpine/helm \
            upgrade --install jobmon-reapers /apps/. \
            -n "$K8S_REAPER_NAMESPACE" \
            --set global.jobmon_container_uri="$JOBMON_CONTAINER_URI" \
            --set global.jobmon_version="$JOBMON_VERSION" \
            --set global.namespace="$K8S_NAMESPACE" \
            --set global.rancher_slack_secret="$RANCHER_SLACK_SECRET" \
            --set global.reaper_namespace="$K8S_REAPER_NAMESPACE"
    fi
}


test_k8s_uge_deployment () {
    WORKSPACE=$1
    QLOGIN_ACTIVATE=$2
    JOBMON_VERSION=$3
    TARGET_IP=$4

    CONDA_DIR=$WORKSPACE/.conda_env/load_test
    $QLOGIN_ACTIVATE && \
        conda create --prefix $CONDA_DIR python==3.8
    $QLOGIN_ACTIVATE &&
       conda activate $CONDA_DIR && \
       pip install pyyaml && \
       pip install jobmon==$JOBMON_VERSION && \
       pip install jobmon_uge && \
       pip install jobmon_slurm && \
       jobmon update_config --web_service_fqdn $TARGET_IP --web_service_port 80 && \
       python $WORKSPACE/deployment/tests/six_job_test.py

#    $QLOGIN_ACTIVATE &&
#        /bin/bash /ihme/singularity-images/rstudio/shells/execRscript.sh -s $WORKSPACE/jobmonr/deployment/six_job_test.r \
#           --python-path $CONDA_DIR/bin/python --jobmonr-loc $WORKSPACE/jobmonr/jobmonr

}

test_k8s_slurm_deployment () {
    WORKSPACE=$1
    MINICONDA_PATH=$2
    CONDA_ENV_NAME=$3
    JOBMON_VERSION=$4
    TARGET_IP=$5

# Do not use the "source" command, because dash does not have it.
# The default login shell on Ubuntu is dash.
# "Source" and "." are synonyms for the same command.
    . ${MINICONDA_PATH} ${CONDA_ENV_NAME} && \
      conda info --envs && \
      conda deactivate && \
      conda env remove --prefix $CONDA_DIR_SLURM python==3.8 && \
      conda info --envs && \
      CONDA_DIR_SLURM=$WORKSPACE/.conda_env/load_test_slurm && \
      conda create --prefix $CONDA_DIR_SLURM python==3.8 && \
      conda activate $CONDA_DIR_SLURM && \
      conda info --envs && \
      pip install pyyaml && \
      pip install jobmon==$JOBMON_VERSION && \
      pip install slurm_rest && \
      pip install jobmon_uge && \
      pip install jobmon_slurm && \
      PATH=$PATH:/opt/slurm/bin && \
      pip freeze && \
      jobmon update_config --web_service_fqdn $TARGET_IP --web_service_port 80 && \
      srun -n 1 -p all.q -A general -c 1 --mem=300 --time=100 python $WORKSPACE/deployment/tests/slurm/six_job_test.py
}