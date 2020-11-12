Architecture
===========================================

Guppy Initial Release Architecture
----------------------------------

.. Requires graphviz binaries on doc build host
.. mac: brew install graphviz
.. graphviz::

  digraph G {
    label="Jobmon Guppy Architecture"
    rankdir=LR; // Left to right direction
    compound=true;
    labelloc="t";

    subgraph "cluster_external" {
        graph[style=solid; color=red];
        label="External Services"
        "slack" [shape="oval"]
    }

    subgraph "cluster_grid_engine" {
        graph[style=solid; color=red];
        label="Grid Engine Cluster";
        subgraph "cluster_qsub" {
            graph [color="blue"]
          label="Processes running in qsub"
          "jobmon clients" ["shape"="tripleoctagon"]
          "jobmon tasks" ["shape"="tripleoctagon"]
        }
      "grid engine scheduler" [shape="oval"]
      "jobmon clients" -> "grid engine scheduler"

    }
    subgraph cluster_qpid_db {
      graph[style=solid; color=red];
      label="QPID DB Host"
      "qpid database" [shape="cylinder"]
    }
    subgraph "cluster_docker-host" {
      graph[style=solid; color=red];
      label="Docker Host Containers";
      "jobmon database" [shape="cylinder"]
      subgraph "cluster_jobmon-container" {
        graph[style=solid; color=blue];
        label="Jobmon Docker Container";
        subgraph "cluster_supervisord" {
          label="supervisord processes";
          graph[style=solid; color=brown];
          "nginx" [shape="octagon"]
          "uwsgi" [shape="octagon"]
          "flask" [shape="octagon"]
          "Jobmon State Manager" [shape="tripleoctagon"]
          "Jobmon Query Service" [shape="tripleoctagon"]
          "Job visualization server" [shape="tripleoctagon"]
          {rank=same; "Jobmon State Manager"; "Jobmon Query Service"}
        }
      }
      "qpid integration" [shape="octagon"]
      "nginx" -> "uwsgi" [label="reverse proxies on unix socket"]
      "uwsgi" -> "flask" [label="runs flask child processes"]
      "flask" -> "Jobmon State Manager"
      "flask" -> "Jobmon Query Service"
      "flask" -> "Job visualization server"
      "jobmon clients" -> "nginx"
      "jobmon tasks" -> "nginx"
      "Jobmon State Manager" -> "jobmon database" [label="Read-Write"]
      "Jobmon Query Service" -> "jobmon database" [label="Read-Only"]
      "Job visualization server" -> "jobmon database" [label="Read-Only"]
      "workflow reaper" -> "nginx"

      "Jobmon State Manager" -> "jobmon clients"
    }


    "workflow reaper" -> "slack"
    "qpid integration" -> "qpid database"
    "qpid integration" -> "jobmon database"  [label="ETL Max-RSS Values"]
  }

Kubernetes Deployment Architecture
----------------------------------

.. graphviz::

  digraph G {
    label="Jobmon Guppy Architecture"
    node [shape=box]
    rankdir=LR; // Left to right direction
    // compound=true;
    newrank=true;
    labelloc="t";

    subgraph cluster_external {
      graph[style=solid; color=red];
      clusterrank=global
      label="External Services"
      "slack" [shape="oval"]
    }

    subgraph cluster_grid_engine {
      graph[style=solid; color=red];
      label="Grid Engine Cluster";
      clusterrank=global
      subgraph cluster_qsub {
        graph [color="blue"]
        label="Processes running in qsub"
        "jobmon_clients" ["shape"="tripleoctagon", label="Jobmon Clients"]
        "jobmon_tasks" ["shape"="tripleoctagon", label="Jobmon Tasks"]
      }
      "grid_engine_scheduler" [shape="oval", label="Grid Enginer Scheduler"]
      "grid_engine_scheduler" -> "jobmon_tasks"
      "jobmon_clients" -> "grid_engine_scheduler"
    }

    subgraph cluster_qpid_db {
      graph[style=solid; color=red];
      clusterrank=global
      label="QPID DB Host"
      "qpid_database" [shape="cylinder", label="QPID DB"]
    }

    subgraph cluster_telemetry_db {
      graph[style=solid; color=red];
      clusterrank=global
      label="Telemetry DB"
      "telemetry_db" [shape="cylinder", label="Telemetry DB (InfluxDB)"]
    }

    subgraph cluster_jobmon_db {
      graph[style=solid; color=red];
      clusterrank=global
      label="Jobmon DB Host"
      "jobmon_database" [shape="cylinder", label="Jobmon Database"]
    }

    subgraph cluster_kubernetes_cluster {
      graph[style=solid; color=red];
      label="Kubernetes Cluster Hosts";

      subgraph cluster_qpid_integration_pod {
        graph[style=solid; color=blue];
        label="QPID Integration Pod";
        subgraph cluster_qpid_integration_container {
          label="QPID Integration Container (one)";
          graph[style=solid; color=brown;];
          "qpid_integration" [shape="octagon", label="QPID Integration Service"]
        }
      }

      subgraph cluster_workflow_reaper_pod {
        graph[style=solid; color=blue];
        label="Workflow Reaper Pod";
        subgraph cluster_workflow_reaper_container {
          label="Workflow Reaper Container (one)";
          graph[style=solid; color=brown;];
          "workflow_reaper" [shape="octagon", label="Workflow Reaper"]
        }
      }

      subgraph cluster_metal_lb_service {
        graph[style=solid; color=blue];
        label="MetalLB Service";
        "metal_lb_service" [shape="oval", label="MetalLB Service (K8s Entrypoint)"]
      }

      subgraph cluster_lb_pod {
        graph[style=solid; color=blue];
        label="Load Balancer Pod";
        subgraph cluster_lb_container {
          label="Load Balancer Containers (many)";
          graph[style=solid; color=brown;];
          "traefik_reverse_proxy" [shape="tripleoctagon", label="Traefik Reverse Proxies"]
        }
      }

      subgraph cluster_jobmon_query_pod {
        graph[style=solid; color=blue];
        subgraph cluster_query_container {
        label="Query Service Containers (many)";
        graph[style=solid; color=brown;];
          label="Job Query Service Pod";
          subgraph cluster_query_supervisord {
            label="supervisord processes";
            graph[style=solid; color=cyan];
            "query_nginx" [shape="octagon", label="Query Service NGINX"]
            "query_uwsgi" [shape="octagon", label="Query Service uWSGI"]
            "query_flask" [shape="octagon", label="Query Service Flask"]
            "query_app" [shape="tripleoctagon", label="Jobmon Query Service"]
            "query_nginx" -> "query_uwsgi" [label="reverse proxies on unix socket"]
            "query_uwsgi" -> "query_flask" [label="runs flask child processes"]
            "query_flask" -> "query_app"
          }
        }
      }

      subgraph cluster_jobmon_state_manager_pod {
        graph[style=solid; color=blue];
        label="Jobmon State Manager Pod";
        subgraph cluster_state_manager_container {
        label="State Manager Containers (many)";
        graph[style=solid; color=brown;];
          subgraph cluster_state_manager_supervisord {
            label="supervisord processes";
            graph[style=solid; color=cyan];
            "state_manager_nginx" [shape="octagon", label="State Manager NGINX"]
            "state_manager_uwsgi" [shape="octagon", label="State Manager uWSGI"]
            "state_manager_flask" [shape="octagon", label="State Manager Flask"]
            "state_manager_app" [shape="tripleoctagon", label="Job State Manager"]
            "state_manager_nginx" -> "state_manager_uwsgi" [label="reverse proxies on unix socket"]
            "state_manager_uwsgi" -> "state_manager_flask" [label="runs flask child processes"]
            "state_manager_flask" -> "state_manager_app"
          }
        }
      }

      subgraph cluster_jobmon_viz_server_pod {
        graph[style=solid; color=blue];
        label="Jobmon Vizualization Server Pod";
        subgraph cluster_viz_container {
          label="Vizualization Containers (many)";
          graph[style=solid; color=brown;];
          subgraph cluster_viz_supervisord {
            label="supervisord processes";
            graph[style=solid; color=cyan];
            "viz_nginx" [shape="octagon", label="Vizualization NGINX"]
            "viz_uwsgi" [shape="octagon", label="Vizualization uWSGI"]
            "viz_flask" [shape="octagon", label="Vizualization Flask"]
            "viz_app" [shape="tripleoctagon", label="Job visualization server"]
            "viz_nginx" -> "viz_uwsgi" [label="reverse proxies on unix socket"]
            "viz_uwsgi" -> "viz_flask" [label="runs flask child processes"]
            "viz_flask" -> "viz_app"
          }
        }
      }
  }
  {rank=same; qpid_integration; workflow_reaper; traefik_reverse_proxy}
  {rank=same; jobmon_database; qpid_database; telemetry_db; slack}
  "qpid_integration" -> "qpid_database"
  "qpid_integration" -> "jobmon_database"  [label="ETL Max-RSS"]
  "state_manager_app" -> "jobmon_database" [label="Read-Write"]
  "jobmon_clients" -> "metal_lb_service"
  "metal_lb_service" -> "traefik_reverse_proxy"
  "traefik_reverse_proxy" -> "query_nginx" [label="Proxy HTTP Requests"]
  "traefik_reverse_proxy" -> "state_manager_nginx" [label="Proxy HTTP Requests"]
  "traefik_reverse_proxy" -> "viz_nginx" [label="Proxy HTTP Requests"]
  "traefik_reverse_proxy" -> "telemetry_db" [label="NGINX Writes Telemetry"]

  "workflow_reaper" -> "state_manager_nginx"
  "workflow_reaper" -> "slack"

  "viz_app" -> "jobmon_database" [label="Read-Only"]
  "query_app" -> "jobmon_database"
  "state_manager_app" -> "grid_engine_scheduler"
  }


Autoscaling Behavior
----------------------------------

Jobmon mainly relies on two infrastructure services in order to stay performant under heavy load, namely uWSGI and Kubernetes.

Kubernetes
**********

The main functionality of Kubernetes (k8s) is container orchestration. The first step in deploying Jobmon is to build a Docker image for the Jobmon server code. That image is then used to build a series of Docker containers, which are grouped into **pods**. Each pod represents a subset of the server routes. For example, all /client/* routes are sent to the jobmon-client pod on Kubernetes. Each pod is instantiated with 3 containers, each with a preset CPU/memory resource allocation.

The traefik router will try and equitably distribute incoming data between the associated containers. For example, an incoming series of /client/* routes will be routed between each of the initial 3 client pods. However, the load handled by the Jobmon service is not always equal. In the event of a very large workflow, or a series of concurrent workflows, the client side pods can get overwhelmed with incoming requests, leading to timeouts or lost jobs. Jobmon utilizes the Kubernetes `horizontal autoscaling algorithm <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`_ when it detects heavy load in the containers. As of 11/3/2020, "heavy load" is set `here <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/k8s/31_deployment_jobmon_client.yaml.j2#52-77>`_. Namely, when either CPU or memory is at 80% or more utilization, we can spin up more containers up to a limit of 10. The traefik router will then divert some incoming routes to the newly created containers in order to allow heavily-utilized containers to finish processes off. When the usage spike is over, and container usage dips below some minimum threshhold, the newly spawned containers will then be killed until we only have the three initial containers remaining.

All of the Kubernetes scaling behavior is configurable, per pod, in the associated .yaml files.

uWSGI
**********

uWSGI is a web service used to communicate between the client side application and the server code. In our architecture, uWSGI runs inside each of the docker containers created by Kubernetes [#f1]_ . uWSGI consists of a main process that manages a series of worker processes.


Like the Kubernetes deployment, each container starts with a minimum number of workers as specified `here <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/jobmon/server/deployment/container/uwsgi.ini#35>`_. If a specific container falls under heavy load, uWSGI can utilize a cheaper algorithm to spawn more workers and process the additional incoming requests. There are a variety of cheaper algorithms that can determine when to scale up/down worker processes - Jobmon uses the `busyness algorithm <https://uwsgi-docs.readthedocs.io/en/latest/Cheaper.html#busyness-cheaper-algorithm>`_. Under this specification, busyness is set by average utilization over a given time period. Configurations can be set in the same uwsgi.ini file linked above.

Similarly to the Kubernetes pod autoscaler, the busyness algorithm will create workers to handle a usage spike and spin down workers when usage is low. This is important for two reasons:

1. A container can efficiently process incoming requests with more workers. If there are no free workers to handle a request, it will sit in the queue until a worker frees up. If requests are incoming more quickly than the workers can execute, this can potentially result in long queue wait times and request timeouts.
2. Without worker autoscaling behavior the resource threshholds needed for Kubernetes horizontal autoscaling will not be reached. Remember that Kubernetes defines busyness by container CPU and memory usage. Adding workers directly adds to the CPU usage, and indirectly adds to memory usage by allowing more concurrent data flow. If the additional threads in the container cannot be allocated work due to lack of autoscaling, then the requisite busyness needed in each container won't be reached. Kubernetes does not track the length of the request queue as a busyness parameter.


Full stack demo
**********

Take a simple Jobmon request: we want to manually set the state of a workflow run to be state "E", so the workflow can be retried.

``wfr.update_status("E")``

1. The update_status function constructs the **/swarm/workflow_run/<workflow_run_id>/update_status** route, which is processed by flask on the client side.
2. Flask sends the request to the Kubernetes service
3. The traefik controller routes the request to the swarm pod, then to a "free" container within the pod
  - If all containers are at high capacity, a new container is created.
4. uWSGI, running inside the container, assigns resources to handle the request.
  - The main process either assigns a worker to the request, or instantiates a new worker process to handle the request.
5. The requested python/SQL logic is executed within the worker process, and the returned data is sent back to the main process.
6. The main process sends the returned data back to the client application.


Performance Monitoring
**********

The Kubernetes cluster workload metrics can be tracked on `Rancher <https://k8s.ihme.washington.edu/c/c-99499/monitoring>`_. Regarding autoscaling, the important information to track is the per-pod container workload metrics. The container-specific workloads can be seen by navigating to the jobmon cluster -> namespace (dev or prod) -> pod (client, swarm, visualization, etc.).

The **Workload Metrics** tab displays a variety of time series plots, notably CPU Utilization and Memory Utilization, broken down by container. This allows tracking of what resources are running in each container. When evaluating perforamance during heavy load, it's important to check the utilization metrics to ensure containers are using the right amount of resources. Low utilization means container resources are not being used efficiently, and high utilization means the autoscaler is not behaving properly.

The **Events** tab will track notifications of when pods are created or spun down based on the horizontal autoscaler. During periods of heavy load, it's important to check that containers are indeed being instantiated correctly, and no containers are getting killed when there is still work to be allocated.

To ensure that routes are being processed efficiently, we can also look at the traefik controller Grafana visualizations. This visualization currently lives at port 3000 of the relevant namespace's IP address. For example, the traefik visualization for the current Jobmon dev deployment lives at http://10.158.146.73:3000/?orgId=1 [#f2]_ . The traefik dashboard can also be accessed from Rancher, by selecting the "3000/tcp" link under the traefik pod.

This visualization will track the number of requests over time, by return code status. We can also see the average 99th percentile response time broken down over a configurable time window. Benchmarks for good performance are:

1. 99th percentile response time is always <1s. Ideally, the average 99th percentile response time does not exceed 500-600 milliseconds.
2. There are very few return statuses of 504. 504 is the HTTP return code for a connection timeout, meaning our request took too long to be serviced. There is built-in resiliency to Jobmon routes, meaning that single-route timeouts are not necessarily fatal for the client. However, consistent timeouts is indicative of a performance bottleneck and can result in lost workflows.


If either of the two above conditions are not met, first check the aforementioned workload metrics and events panels. In the case that Kubernetes autoscaling isn't detecting busyness appropriately, we can actually force manual autoscaling by manually adding containers to the overwhelmed pods. This can be done by incrementing the "config scale" toggle on the pod-specific page.

If container busyness is low but latencies are still high, check the container logs in the Traefik pod to see individual route latencies and identify the bottlenecking route call [#f3]_ .


.. rubric:: Footnotes

.. [#f1] Technically, incoming/outgoing communication to the client is managed by nginx, but since it's not relevant to the autoscaling behavior nginx discussion is omitted here.

.. [#f2] The IP address and port number may change over time, depending on the Kubernetes configuration. Check the metallb repository to confirm the correct IP address.

.. [#f3] As of now, almost all slowness in the server can be attributed to throttled database read/write access. Common solutions are to suggest spacing out workflow instantiation, or binding tasks/nodes in smaller chunks.