Architecture
===========================================

Gubby Initial Release Architecture
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
      "workflow reaper" -> "nginx"

      "Jobmon State Manager" -> "jobmon clients"
    }


    "workflow reaper" -> "slack"
    "qpid integration" -> "qpid database"
    "qpid integration" -> "jobmon database"  [label="ETL Max-RSS Values"]
  }

Kubernetes Deployment Architecture
----------------------------------
