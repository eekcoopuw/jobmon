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