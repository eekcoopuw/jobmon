On Premise Kubernetes Deployments
=================================

Deployment on new cluster (From scratch)
----------------------------------------

1. Create rancher project
2. Update rancher project id default in k8s/deploy.Jenkinsfile
3. Export kube.conf file
4. Add kube.conf file to Jenkins credential Manager
5. Update k8s/deploy.Jenkinsfile to target new kube.conf secret
6. Proceed to "New jobmon deployment in a existing rancher cluster" Section


New jobmon deployment in a existing rancher cluster
---------------------------------------------------

1. Build and publish a docker container to Artifactory using build_docker.Jenkinsfile
2. Update MetalLB configuration to include an IP Pool for the new deployment
3. Run jenkins job that targets k8s/deploy.Jenkinsfile, targeting a new namespace

Note:  If you have created a new Jenkins job, you will either have to manually add the
parameters before first run, or run the job once expecting it to fail, so that Jenkins
can read the parameters from the Jenkinsfile.

Updating an existing deployment
-------------------------------

1. Build and publish a docker container to Artifactory using `Jenkins <https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon-deploy-k8s/>`_.
2. Clear out the existing routes from Traefik.
3. Run jenkins job that targets k8s/deploy.Jenkinsfile, targeting the existing namespace


Deleting  Ingressroutes from Traefik
--------------------------------------------
If k8s is updated Traefik keeps the old routes and adds the new routes.
If we have changed routes then the old ones will still be probed by Traefik
and Traefik's logs will be flooded with errors.
Use `kubectl` to delete old routes, e.g.:

::

    bash$ kubectl -n jobmon-dev get ingressroutes
    NAME                                 AGE
    grafana-routes                       100d
    jobmon-client                        50d
    jobmon-query-service-routes          100d
    jobmon-scheduler                     50d
    jobmon-state-manager-routes          100d
    bash$ kubectl -n jobmon-dev delete ingressroutes/jobmon-query-service-routes
    ingressroute.traefik.containo.us "jobmon-query-service-routes" deleted


Jenkins Jobs
------------

build_docker.Jenkinsfile
^^^^^^^^^^^^^^^^^^^^^^^^

* Link: https://scicomp-jenkins-p02.ihme.washington.edu/job/jobmon-build-docker/
* Jenkins Credentials Used:

  * Artifactory user with write permissions.

* Parameters: None
* Builds docker container; Uploads container to Artifactory; URI Generated from build number and date/time

k8s/deploy.Jenkinsfile
^^^^^^^^^^^^^^^^^^^^^^

* Link: https://scicomp-jenkins-p02.ihme.washington.edu/job/jobmon-deploy-k8s/

* Jenkins Credentials Used:

  * Artifactory user with write permissions: For building grafana container
  * Kubeconf: Access to kubernetes cluster. Export from rancher webui.
  * Jobmon database credentials: Passed as env vars to jobmon containers during deployment
  * Slack Token: API key to send slack messages

* Parameters:

  * K8S_NAMESPACE: Kubernetes Namespace to use and/or create.
  * METALLB_IP_POOL: MetalIP IP Pool to use. Determines which service IP is used for ingress traffic.
  * JOBMON_CONTAINER_URI: Container URI from build_docker Jenkins Job.
  * DB_HOST: Jobmon database hostname. Use a FQDN.  (Uses username and password from jenkins secret specified in Jenkinsfile)
  * DB_PORT: Jobmon database port number.
  * RANCHER_PROJECT_ID: Rancher project ID. Make a project in the webui and get the project ID from the URL.
  * NODE_SLACK_CHANNEL: Slack Node Channel (Uses api key from jenkins secret specified in Jenkinsfile)
  * WF_SLACK_CHANNEL: Slack Workflow Channel (Uses api key from jenkins secret specified in Jenkinsfile)

.. graphviz::

  digraph G {
    label="Jobmon Guppy Deployment Process"
    rankdir=LR; // Left to right direction
    compound=true;
    labelloc="t";
    node [shape="box"];

    "Trigger build docker container" [shape=oval]
    "Application is online" [shape=oval]

    "Trigger build docker container"->"Docker build runs"
    "Docker build runs" -> "Docker Image Uploads to Artifactory"
    "Docker Image Uploads to Artifactory" -> "Record Image URI"

    "Record Image URI" -> "Populate Deployment Job Parameters"
    "Populate Deployment Job Parameters" -> "Trigger Build of Deployment"

    "Trigger Build of Deployment" -> "Grafana Image Created"
    "Grafana Image Created" -> "Grafana Image Uploads to Artifactory"
    "Grafana Image Uploads to Artifactory" -> "Kubernetes Templates Rendered"
    "Kubernetes Templates Rendered" -> "Namespace created if it does not exsit"
    "Namespace created if it does not exsit" -> "Template applied with kubectl in numberic order"
    "Template applied with kubectl in numberic order" -> "Kubernetes starts containers"
    "Kubernetes starts containers" -> "Application is online"
  }

Optional Components
-------------------

1. Configure centralized logging from Rancher UI.

  a. In the Rancher WebUI navigate to <Cluster>/Tools/Logging.
  b. Select syslog
  c. Populate syslog values. Example below:

     .. image:: images/k8s_syslog_configuration.png

  d. Save

2. Configure Cluster Level Monitoring

  a. In the Rancher WebUI navigate to <Cluster>/Tools/Monitoring
  b. Populate monitoring values. Example Below:

    .. image:: images/k8s_cluster_level_monitoring.png

  c. Save


3. Configure Project Level Container Monitoring

  a. In the Rancher WebUI navigate to <Cluster>/<Project>, then Tools/Monitoring
  b. Populate monitoring values. Example Below:

    .. image:: images/k8s_project_level_monitoring.png

  c. Save