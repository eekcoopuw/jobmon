
get_metallb_cfg () {
    KUBECTL_CONTAINER=$1
    WORKSPACE=$2

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
    TARGET_IP=$2
    REG_USERNAME=$3
    REG_PASSWORD=$4
    ACTIVATE=$5

    INI=$WORKSPACE/src/jobmon/.jobmon.ini
    rm $INI
    echo -e "[client]\nweb_service_fqdn=$TARGET_IP\nweb_service_port=80" > $INI
    $ACTIVATE && nox --session distribute
    PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
    $ACTIVATE && twine upload \
        --repository-url $PYPI_URL \
        --username $REG_USERNAME \
        --password $REG_PASSWORD \
        ./dist/*
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
    echo "jobmon==$JOBMON_VERSION" > $WORKSPACE/requirements.txt
    docker login -u "$REG_USERNAME" -p "$REG_PASSWORD" "https://$SCICOMP_DOCKER_REG_URL"
    docker build --no-cache -t "$JOBMON_CONTAINER_URI" -f ./deployment/k8s/Dockerfile .
    docker push "$JOBMON_CONTAINER_URI"

    # build grafana container
    docker build --no-cache -t "$GRAFANA_CONTAINER_URI" -f ./deployment/k8s/grafana/Dockerfile .
    docker push "$GRAFANA_CONTAINER_URI"

}


deploy_jobmon_to_k8s () {
    YASHA_CONTAINER=${1}
    WORKSPACE=${2}
    JOBMON_CONTAINER_URI=${3}
    METALLB_IP_POOL=${4}
    K8S_NAMESPACE=${5}
    RANCHER_PROJECT_ID=${6}
    GRAFANA_CONTAINER_URI=${7}
    RANCHER_DB_SECRET=${8}
    RANCHER_SLACK_SECRET=${9}
    RANCHER_QPID_SECRET=${10}
    KUBECTL_CONTAINER=${11}
    KUBECONFIG=${12}
    USE_LOGSTASH=${13}

    # Deploy the ELK stack to K8S
    docker pull alpine/helm  # Pull prebuilt helm container

    # Remove ELK instance if already present
    docker run -t \
        --rm \
        -v "$WORKSPACE/deployment/elk:/apps" \
        -v $KUBECONFIG:/root/.kube/config \
        alpine/helm uninstall jobmon-elk || true

    # Deploy ELK
    docker run -t \
        --rm \
        -v "$WORKSPACE/deployment/elk:/apps" \
        -v $KUBECONFIG:/root/.kube/config \
        alpine/helm \
            install jobmon-elk /apps/. \
            --set global.namespace="$K8S_NAMESPACE"

    # Render each .yaml.j2 template in the k8s dir (return only the basename)
    docker pull $YASHA_CONTAINER
    for TEMPLATE in $(find "$WORKSPACE/deployment/k8s/" -maxdepth 1 -type f -name '*.yaml.j2' -printf "%f\n"|sort -n)
    do
        docker run -t \
            --rm \
            -v "$WORKSPACE/deployment/k8s:/data" \
            $YASHA_CONTAINER \
                --jobmon_container_uri="$JOBMON_CONTAINER_URI" \
                --ip_pool="$METALLB_IP_POOL" \
                --namespace="$K8S_NAMESPACE" \
                --rancherproject="$RANCHER_PROJECT_ID" \
                --grafana_image="$GRAFANA_CONTAINER_URI" \
                --rancher_db_secret="$RANCHER_DB_SECRET" \
                --rancher_slack_secret="$RANCHER_SLACK_SECRET" \
                --rancher_qpid_secret="$RANCHER_QPID_SECRET" \
                --use_logstash="$USE_LOGSTASH" \
                /data/${TEMPLATE}
    done

    chown -R "$(id -u):$(id -g)" .

    docker pull $KUBECTL_CONTAINER
    docker run -t \
        --rm \
        -v $KUBECONFIG:/root/.kube/config \
        -v "$WORKSPACE/deployment/k8s:/data" \
        $KUBECTL_CONTAINER \
            get namespace "$K8S_NAMESPACE" ||
    docker run -t \
        --rm \
        -v $KUBECONFIG:/root/.kube/config \
        -v "$WORKSPACE/deployment/k8s:/data" \
        $KUBECTL_CONTAINER \
            apply -f /data/01_namespace.yaml
    # Remove the rendered namespace setup yaml before proceeding
    rm -f ./deployment/k8s/01_namespace.yaml

    # Deploying deployment and service configurations to k8s
    for TEMPLATE in $(find "$WORKSPACE/deployment/k8s/" -maxdepth 1 -type f -name '*.yaml' -printf "%f\n" |sort -n)
    do
        docker run -t \
            --rm \
            -v $KUBECONFIG:/root/.kube/config \
            -v "$WORKSPACE/deployment/k8s:/data" \
            ${KUBECTL_CONTAINER} \
                --namespace="$K8S_NAMESPACE" \
                apply -f /data/${TEMPLATE}
        sleep 5 # Rest for 5 seconds to let the pods calm down a bit
    done
}


test_k8s_deployment () {
    WORKSPACE=$1
    QLOGIN_ACTIVATE=$2
    JOBMON_VERSION=$3

    CONDA_DIR=$WORKSPACE/.conda_env/load_test
    $QLOGIN_ACTIVATE && \
        conda create --prefix $CONDA_DIR python==3.7
    $QLOGIN_ACTIVATE &&
        conda activate $CONDA_DIR && \
        pip install jobmon==$JOBMON_VERSION && \
        python $WORKSPACE/deployment/tests/six_job_test.py
}
