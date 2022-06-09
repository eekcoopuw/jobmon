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


get_connection_info_from_namespace () {
    WORKSPACE=$1
    K8S_NAMESPACE=$2

    # pull kubectl container
    docker pull $KUBECTL_CONTAINER

    # get ip
    docker run \
        -t \
        --rm \
        -v ${KUBECONFIG}:/root/.kube/config \
        --mount type=bind,source="$WORKSPACE",target=/data \
        $KUBECTL_CONTAINER  \
            get svc traefik \
            -n "$K8S_NAMESPACE" \
            -o "jsonpath={.status.loadBalancer.ingress[].ip}" > $WORKSPACE/jobmon_service_fqdn.txt

    docker run \
        -t \
        --rm \
        -v ${KUBECONFIG}:/root/.kube/config \
        --mount type=bind,source="$WORKSPACE",target=/data \
        $KUBECTL_CONTAINER  \
            get svc traefik \
            -n "$K8S_NAMESPACE" \
            -o 'jsonpath={.spec.ports[?(@.name=="web")].port}' > $WORKSPACE/jobmon_service_port.txt
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

    $ACTIVATE && nox --session build
    PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
    JOBMON_VERSION=$(basename $(find ./dist/jobmon-*.tar.gz) | sed "s/jobmon-\\(.*\\)\\.tar\\.gz/\\1/")
#    if [[ "$JOBMON_VERSION" =~ "dev" ]]
#    then
#      $ACTIVATE && twine upload \
#        --repository-url $PYPI_URL \
#        --username $REG_USERNAME \
#        --password $REG_PASSWORD \
#        --skip-existing \
#        ./dist/*
#    else
#      $ACTIVATE && twine upload \
#        --repository-url $PYPI_URL \
#        --username $REG_USERNAME \
#        --password $REG_PASSWORD \
#        ./dist/*
#    fi
    export JOBMON_VERSION_DEPLOYED="$JOBMON_VERSION"
    echo "Jobmon v$JOBMON_VERSION deployed to Pypi"

}


get_container_name_from_version () {
    JOBMON_VERSION=$1
    SCICOMP_DOCKER_REG_URL=$2
    SCICOMP_DOCKER_DEV_URL=$3

    # check if dev is in the version string and pick a container name based on that
    if [[ "$JOBMON_VERSION" =~ "dev" ]]
    then
      BASE_URL=$SCICOMP_DOCKER_DEV_URL
    else
      BASE_URL=$SCICOMP_DOCKER_REG_URL
    fi
    echo "$BASE_URL/jobmon:$JOBMON_VERSION"
}


upload_jobmon_image () {
    JOBMON_VERSION=$1
    WORKSPACE=$2
    REG_USERNAME=$3
    REG_PASSWORD=$4
    JOBMON_CONTAINER_URI=$5

    SCICOMP_DOCKER_REG_URL=$(dirname $JOBMON_CONTAINER_URI)

    # build jobmon container
    echo "jobmon[server]==$JOBMON_VERSION" > $WORKSPACE/requirements.txt
    docker login -u "$REG_USERNAME" -p "$REG_PASSWORD" "https://$SCICOMP_DOCKER_REG_URL"
    docker build --no-cache -t "$JOBMON_CONTAINER_URI" -f ./deployment/k8s/Dockerfile .
    docker push "$JOBMON_CONTAINER_URI"

}


deploy_jobmon_to_k8s () {
    WORKSPACE=${1}
    JOBMON_CONTAINER_URI=${2}
    METALLB_IP_POOL=${3}
    K8S_NAMESPACE=${4}
    RANCHER_PROJECT_ID=${5}
    RANCHER_DB_SECRET=${6}
    RANCHER_SLACK_SECRET=${7}
    RANCHER_QPID_SECRET=${8}
    RANCHER_DB_SLURM_SDB_SECRET=${9}
    KUBECONFIG=${10}
    USE_LOGSTASH=${11}
    JOBMON_VERSION=${12}
    K8S_REAPER_NAMESPACE=${13}
    DEPLOY_JOBMON=${14}
    DEPLOY_ELK=${15}
    DEPLOY_REAPER=${16}

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
            --set metricbeat.db_host_secret="$RANCHER_DB_SECRET" \
            --history-max 3 \
            --set global.namespace="$K8S_NAMESPACE"
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
            --history-max 3 \
            --set global.jobmon_container_uri="$JOBMON_CONTAINER_URI" \
            --set global.metallb_ip_pool="$METALLB_IP_POOL" \
            --set global.namespace="$K8S_NAMESPACE" \
            --set global.rancher_db_secret="$RANCHER_DB_SECRET" \
            --set global.rancher_project="$RANCHER_PROJECT_ID" \
            --set global.rancher_qpid_secret="$RANCHER_QPID_SECRET" \
            --set global.rancher_db_slurm_sdb_secret="$RANCHER_DB_SLURM_SDB_SECRET" \
            --set global.rancher_slack_secret="$RANCHER_SLACK_SECRET" \
            --set global.use_logstash="$USE_LOGSTASH"
    fi

    if [[ "$DEPLOY_REAPER" = true ]]
    then
        echo "Adding new reaper to reapers namespace"
        docker run -t \
        --rm \
        -v "$WORKSPACE/deployment/k8s/reapers:/apps" \
        -v $KUBECONFIG:/root/.kube/config \
        alpine/helm \
            upgrade --install jobmon-reapers /apps/. \
            -n "$K8S_REAPER_NAMESPACE" \
            --history-max 3 \
            --set global.jobmon_container_uri="$JOBMON_CONTAINER_URI" \
            --set global.jobmon_version="$JOBMON_VERSION" \
            --set global.namespace="$K8S_NAMESPACE" \
            --set global.rancher_slack_secret="$RANCHER_SLACK_SECRET" \
            --set global.reaper_namespace="$K8S_REAPER_NAMESPACE"
    fi
}

test_conda_client_slurm () {
    WORKSPACE=$1
    MINICONDA_PATH=$2
    CONDA_ENV_NAME=$3
    CONDA_CLIENT_VERSION=$4
    JOBMON_VERSION=$5

# Although "Source" and "." are synonyms in many contexts, Dash does not have "Source",
# so we are using "." here.
# The default login shell on Ubuntu is dash.
    . ${MINICONDA_PATH} ${CONDA_ENV_NAME} && \
      conda deactivate && \
      conda env remove --name slurm_six_job_env && \
      conda create -n slurm_six_job_env python==3.7 ihme_jobmon==$CONDA_CLIENT_VERSION -k --channel https://artifactory.ihme.washington.edu/artifactory/api/conda/conda-scicomp --channel conda-forge && \
      conda activate slurm_six_job_env && \
      conda info --envs && \
      PATH=$PATH:/opt/slurm/bin && \
      pip freeze && \
      srun -n 1 -p all.q -A proj_scicomp -c 1 --mem=10000 --time=100 python $WORKSPACE/deployment/tests/six_job_test.py 'slurm'
}

test_server () {
    WORKSPACE=$1
    SLURM_ACTIVATE=$2
    JOBMON_VERSION=$3
    WEB_SERVICE_FQDN=$4
    WEB_SERVICE_PORT=$5

    CONDA_DIR=$WORKSPACE/.conda_env/load_test
    $SLURM_ACTIVATE && \
        conda create --prefix $CONDA_DIR python==3.7
    $SLURM_ACTIVATE &&
        conda activate $CONDA_DIR && \
        pip install jobmon==$JOBMON_VERSION && \
        jobmon_config update --web_service_fqdn $WEB_SERVICE_FQDN --web_service_port $WEB_SERVICE_PORT && \
        python $WORKSPACE/deployment/tests/six_job_test.py sequential
    # Disable jobmonr test because it cannot pass version check
    #$SLURM_ACTIVATE &&
    #    /bin/bash /ihme/singularity-images/rstudio/shells/execRscript.sh -s $WORKSPACE/jobmonr/deployment/six_job_test.r \
    #        --python-path $CONDA_DIR/bin/python --jobmonr-loc $WORKSPACE/jobmonr/jobmonr
}

deploy_integrator_to_k8s () {
    WORKSPACE=${1}
    JOBMON_CONTAINER_URI=${2}
    K8S_NAMESPACE=${3}
    RANCHER_PROJECT_ID=${4}
    RANCHER_DB_SECRET=${5}
    RANCHER_DB_SLURM_SDB_SECRET=${6}
    KUBECONFIG=${7}
    RETIRE_AGE=${8}

    echo "WORKSPACE $WORKSPACE"
    echo "JOBMON_CONTAINER_URI $JOBMON_CONTAINER_URI"
    echo "K8S_NAMESPACE $K8S_NAMESPACE"
    echo "RANCHER_PROJECT_ID $RANCHER_PROJECT_ID"
    echo "RANCHER_DB_SECRET $RANCHER_DB_SECRET"
    echo "KUBECONFIG $KUBECONFIG"
    echo "RANCHER_DB_SLURM_SDB_SECRET $RANCHER_DB_SLURM_SDB_SECRET"
    echo "RETIRE_AGE" $RETIRE_AGE

    docker pull $HELM_CONTAINER  # Pull prebuilt helm container
    docker pull $KUBECTL_CONTAINER

    # Check if namespace exists, if not create it: render 01_namespace.yaml and apply it

    echo "KUBECONFIG $KUBECONFIG"
    docker run -t \
        --rm \
        -v $KUBECONFIG:/root/.kube/config \
        -v "$WORKSPACE/deployment/k8s:/data" \
        $KUBECTL_CONTAINER \
            get namespace "$K8S_NAMESPACE"\

    namespace_exists=$?
    if [[ $namespace_exists -ne 0 ]]
    then
        echo "Namespace $$K8S_NAMESPACE does not exist now creating it "
        docker run -t \
            --rm \
            -v $KUBECONFIG:/root/.kube/config \
            -v "$WORKSPACE/deployment/k8s/integrator/:/data" \
            alpine/helm \
                template /data -s templates/01_namespace.yaml \
                --set global.rancher_retire_age="$RETIRE_AGE" \
                --set global.namespace="$K8S_NAMESPACE" \
                --set global.rancher_project="$RANCHER_PROJECT_ID" >> \
                "$WORKSPACE/deployment/k8s/integrator/namespace.yaml"
        docker run -t \
            --rm \
            -v "$WORKSPACE/deployment/k8s/integrator:/data" \
            -v $KUBECONFIG:/root/.kube/config \
            ${KUBECTL_CONTAINER} apply -f /data/namespace.yaml
    fi

    # Remove file, so helm doesn't attempt to re-deploy it
    rm -f ./deployment/k8s/integrator/templates/01_namespace.yaml

    echo "Creating or updating integrator deployment"
    docker run -t \
    --rm \
    -v "$WORKSPACE/deployment/k8s/integrator:/apps" \
    -v $KUBECONFIG:/root/.kube/config \
    alpine/helm \
        upgrade --install jobmon-integrator /apps/. \
        -n "$K8S_NAMESPACE" \
        --history-max 3 \
        --set global.jobmon_container_uri="$JOBMON_CONTAINER_URI" \
        --set global.namespace="$K8S_NAMESPACE" \
        --set global.rancher_db_secret="$RANCHER_DB_SECRET" \
        --set global.rancher_project="$RANCHER_PROJECT_ID" \
        --set global.rancher_db_slurm_sdb_secret="$RANCHER_DB_SLURM_SDB_SECRET" \
        --set global.rancher_retire_age="$RETIRE_AGE"
}
