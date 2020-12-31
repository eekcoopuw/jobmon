Architecture
############

Jobmon's Vision
***************

Make it as easy as possible for everyone at IHME to run any kind of code
on any compute platform, reliably, efficiently, without heroic effort.

Allow people to sleep easily on the
weekend because they do not have to manually monitor their applications.

Jobmon exists because UGE is not reliable and is difficult to use.
IHME was unable to reliably run applications with more than perhaps 10,000 jobs
without heroic and unreliable work practises.

Features
********

Current (as of 2.0)
===================

- Easier monitoring of Python applications through centralization in a sql database
- Finer-grained job dependencies
- Automatic Job Retries
- Resource-retries
- Whole-of-Application Resumes
- Kubernetes deployment
- Meta-Type foundation for Resource Prediction
- Deployment on Kubernetes and a Simple Docker System

Future
======

- R Control Script
- Resource Prediction
- GUI for code-less operations
- Stand-alone deployment on a laptop using Docker
- Operate on Azure Batch
- Operate on SLURM

Requirements View
*****************

Roles
=====

A Role (aka Actor) is a human or an external system that interacts with Jobmon.
Technically, a Role is a Domain Object that can initiate a Use Case.
Most roles are human, but some system roles exist because they initiate a use case.
For example, the UGE scheduler is a system role because it initiates the Use Case "Launch a Job."

One person will often play the part of different Roles during the same day.
For example, at IHME a Coder will often also be an Application Operator.
Therefore Roles are not job titles.

Human Roles
===========

- Python Coder
- R Coder
- Stata Coder
- Other-language Coder
- Python Control Script
- R Control Script
- Application Operator
- Application Owner

System Roles
============

- UGE Scheduler (it starts jobs)
- cgroups (it kills jobs)
- OOM Killer (it also kills jobs if cgroups failes)
- Cluster Scheduler (Broadly UGE, Azure, SLURM)
- The Gremlin (a synthetic System Role, it causes hardware to fail)

Domain Objects
==============

Any noun mentioned in a use case must either be a role or a domain object.
Domain Objects are typically capitalized to show that they are defined terms.
Domain objects might not be implemented in code. For example, there is currently
no "cluster" object, although Jobmon could need it when it has multiple executors.
A domain object might be implemented by different pieces of code, depending on its
location in the deployment architecture. For example, domain objects such as Workflow
are implemented in the database schema, the sqlalchemy model objects, as a wire format,
and as a stub in the Python client and the experimental R client.

All domain objects are defined in the :doc:`glossary`

Domain Objects that are mentioned in the Use Cases but are not part of Jobmon:

- UGE job
- Linux Process
- Conda environment
- Cluster Node
- Python Application
- R Application

Use Cases
=========
Use Cases all follow the naming pattern:

*<Role> <verb> <Domain-Object Phrase>*

For example:

- UGE launches Job
- Python-Application Creates Workflow
- Python-Application Runs Workflow
- Gremlin breaks a Cluster Node


In a waterfall project this Use Case section would be much bigger. Jobmon was developed using
the agile process, therefore the requirements were defined along the way.
The use cases identified here are looking forward to an operating GUI.


Coder Use Cases
===============

100. Coder Converts a Qsub Control Script to Jobmon

Included to emphasize the importance of usability, this use case will describe the extra steps that are necessary


Application Operator Use Cases
==============================

210. Application Operator Starts Application

220. Application Operator Monitors Application

How is it going? Are there any Failures? When will it be done?
Originally they looked in the database. Now they use a CLI. A GUI would be good.

230. Application Operator Debugs Application

How do they find the task statuses? Errors from their own applications

Jobmon Scheduler Use Cases
==========================

330. Jobmon submits a Job to UGE

Key use case. Discuss bash scripts, Python Tasks (split the use case?), What a qsub command must look like

UGE Use Cases
=============

410. UGE Job starts

Phone homes, launches UGE sub-process

420. UGE Job finishes, with or without error

430. Cgroups kills a UGE for excess Resource Usage

Constraints and Non-functional Requirements (aka -ilities)
**********************************************************

Scaling
=======

The goal will be to run "all jobs" on the cluster.
The current largest workflow is the Burdenator, with about 500k jobs.
Twice we have seen workflows of size about 1.5 million, although they are arguably
 over-parallelized.
On IHME's cluster Jobmon should plan for 20% annual growth in all dimensions.

+-----------+-----------------------+---------------------------+---------------------------+
| Date      |	Largest workflow    | Simultaneous workflows    | Transactions per Second   |
+===========+=======================+===========================+===========================+
| Jan 2021  |	500k                |                           |                           |
+-----------+-----------------------+---------------------------+---------------------------+
| June 2021 |	1 million           |                           |                           |
+-----------+-----------------------+---------------------------+---------------------------+


Security
========
Security does not have to be especially high, except that it should not be possible to use
Jobmon to launch bad-actor jobs on the cluster. Therefore exposing a service to the internet
that allows Jobmon to run jobs on the cluster would be a big security risk. Jobmon relies
on existing IHME security systems.

Jobmon stores no data apart from commands, so the cost of
a data breach would be low.

Lifetime Maintainability
========================
Plan for a 5-10 year lifetime

Portability
===========
Jobmon was designed and developed as a seqqunece of Minimal Viable Product releases, so it was not
designed to be a cross-platform system. However, it is highly portable because it only depends
on Python, web technologies, sql, and the cluster OS is abstracted behind the Executor API.

MPI support could be difficult.

GPUs can be supported if they are implemented in separate queues in the cluster OS.

Usability
=========

Usability is key, otherwise Jobmon will not be adopted.
It must be easier than raw UGE, preferably easier than Azure Bath Service and SLURM.
However, we have no experience with SLURM and it might not have the usability problems
present in UGE. Specifically:

Retries: UGE has one global setting for the number of retries, Jobmon allows the number of retires to be set per task.


Logical View (aka software layers, Component View)
**************************************************

What is a Component?


Components are mini-products. Control and responsibility are their defining characteristics.

In the source control system a component is one directory tree.
It contains every kind of code needed for that component: python, sql, javascript, etc.

Suppose we needed to add authentication and authorisation to the rate limiting feature in jobmon.
For this example, also assume that we could not find an existing external system for people,
organizations, and their relationships.
Therefore we need to construct an Organization component that is completely responsible for that area.
It will have uses cases for:

- CRUD a user (full CRUD)
- CRUD a team (full CRUD)
- CRUD an application
- Get escalation path for a user
- Is user authorized to control this application?

CRUD = Create, Read, Update, Delete of a Domain Object.

It needs code at the following layers:

- HTML and Javascript for the CRUD screens
- Python API and then code  for validating CRUD screens, computing escalation paths, authentication etc
- Database tables

The different kinds of code are deployed in different places.
Organize the source tree by the are of responsibility, it makes it easier for a maintenance programmer

FYI CRUD = Create, Read, Update, Delete.

*In hindsight I think thix is a little Hyper-modern: abstractly appealing,
but too fiddly in practise.* Systems rarely need to be so modular that new ones can be
composed from arbitrary subsets. Jobmon is probably one component in its own right, as is
QPIDF, UGE, the organizational component described above.


Components in Guppy
===================

The python packages are currently organized according to the deployment architecture,
not by the major noun, although there is strong correlation.

Perhaps components make sense within a deployment unit,
and this section should be repeated within each of the three deployment groups.


Process View
************

*What calls what.*

UML diagrams to represent process view include the sequence diagram, communication diagram, activity diagram.

*TO DO This section is a sketch*

The Python Client Path
======================
TO DO Trace the call from the User's python code:
1. through jobmons Python library,
#. HTTP to Kubernetees
# Metal-lb
# UWSGI
# Flask
# kubernetes service
# database

The QSUB Path
=============
The whole execution_warpper process, with Popen and exception catches

Resource Retries
================

With the move to the fair (Buster) cluster, resource limits are enforced,
and jobs may die due to cluster enforcement if they have under-requested resources.
In order to help jobs complete without user intervention every time,
Jobmon now has resource adjustment. If it detects that a job has died due to
resource enforcing, the resources will be increased and the job will be retried
if it has not exceeded the maximum attempts.

A record of the resources requested can be found in the executor parameter set
table where each job will have the original parameters requested and the
validated resources as well as rows added each time a resource error occurs
and the resources need to be increased. If this happens, the user should
reconfigure their job to use the resources that ultimately succeeded so that
they do not waste cluster resources in the future.

A step-by-step breakdown of how jobmon deals with a job instance failing due
to resource enforcement is as follows:

1. job instance exits with a resource killed error code
2. The reconciler finds job instances with resource error codes and moves them to state Z.
   The job will be moved into state A (Adjusting Resources) if it has retries available.
3. The job instance factory will retrieve jobs queued for instantiation and
   jobs marked for Adjusting Resources, it will add a new column with adjusted
   resources to the executor parameters set table for that job, and mark
   those as the active resources for that job to use, then it will queue it
   for instantiation using those resources
4. a new job instance will be created, and it will now refer to the new
   adjusted resource values

The query to retrieve all resource entries for all jobs in a dag is::

    SELECT EPS.*
    FROM executor_parameter_set EPS
    JOIN job J on(J.job_id=EPS.job_id)
    WHERE J.dag_id=42;

.. _deployment-view:

Deployment View
***************

*Which pieces of code are deployed where.*

Jobmon is deployed in three place:
- Client, in the same process as the Python control script
- Worker-node, a wrapper container around the actual UGE Task
- Server, as a set of Kubernetes services, defined below

Python Client
=============
This ia standard python wheel that is pip-installable. At run-time it is within the
Application's Python process and is called directly. It communicates via http to the
kubernetes services.

*Strategies aka Executors*

The strategy package is part of the client. It represents the Cluster Operating system.
Jobmon has three at present:
- UGE (aka SGE)
- Sequential (one job after another), and
- Multiprocessing (jobs launched using Python MP)

Only the UGE scheduler is used in production, the other two are useful for testing, and for
the upcoming Jobmon-on-a-laptop deployment.

Be very aware of the difference between where:
1. Jobmon services are deployed (kubernetes or docker), are
2. Where the jobs it controls are running.

**These are two separate axes:
(Kubernetes, Docker) CROSS (UGE, SLURM, Azure, Python-Sequential, Python-MP)**

R-Client & Executor Service
===========================
As of January 202 we are experimenting with an R-client that calls Python immediately
via the R reticulate package. Each Python API call has an R equivalent.
The python interpreter runs in the same process as the R interpreter, so values are passed
directly in memory. The translation overhead is not known.

The second step will be to separate all the machinery that is currently in the Python client
into an ExecutorService that will contain the ``scheduler`` and ``strategies`` packages.
Python and R clients will simply use http to communicate with it when necessary. Calls from
the application that are currently synchronous (e.g. execute dag) will become asynchronous.
The executor service could be deployed locally (using Python MP), or deployed centrally as
a highly-scaled kubernetes container.

Worker-node
===========
The worker_node code is inside the Client package, it should move.
The client package mostly has the code that runs inside the end-users python process,
but it also contains the worker-node code.
If Jobmon was only on a UGE then move it to a new top-level package, named worker-node.
However, UGE and Slurm can probably share the same execution_wrapper because they both run on Linux.
Azure needs a different execution wrapper.
What matters is the worker node operating environment (Linux vs docker), not the cluster OS.
Therefore this package will be moved as part of the port to Azure.

Server
======

The server pacakge contains the kubernetes service, plus the model objects for communicating
to the mysql database.

As of 2.0 (Guppy) the Jobmon production server is deployed as a series of Kubernetes containers.
Prior to 1.0.3 Jobmon serves were deplpoyed using docker. That capability will return in 2.2 as
the "Bootable on a Laptop" feature.
Each container is responsible for the routes from one external system or client.
The containers are organized according to the load they carry, and will scale independently:

+-------------------+-----------------------------------------------------+-------------------+
| Container/Package | Description and Comments                            | Domain Objects    |
+===================+=====================================================+===================+
| jobmon-client     | Handles requests from the the python client inside  | Tool, Workflow    |
|                   | the application code.Therefore it creates workflows | Task, Attributes  |
|                   | and tasks. Basically a CRUD service.                | TaskTemplate      |
+-------------------+-----------------------------------------------------+-------------------+
| jobmon-scheduler  | Owns the routes from the executor. The scheduler    | TaskInstance      |
|                   | (which is part of the executor) reports UGE job ids | Executor          |
|                   | and similar. Also has workflow run heartbeat.       | WorkflowRun       |
+-------------------+-----------------------------------------------------+-------------------+
| jobmon-swarm      | Returns jobs of a particular status to the swarm to | WorkflowRun       |
|                   | be used in the DAG traversal algorithm. Closely     |                   |
|                   | related to jobmon-scheduler.                        |                   |
+-------------------+-----------------------------------------------------+-------------------+
| jobmon-worker     | Owns the finite state machine. All UGE tasks on     |                   |
|                   | worker nodes "phone home" when they start, stop etc | TaskInstance      |
+-------------------+-----------------------------------------------------+-------------------+
| jobmon-qpid-      | Calls QPID to get updated TaskInstance resource     |    TaskInstance   |
| integration       | usage. UGE qacct returns bad information.           |                   |
+-------------------+-----------------------------------------------------+-------------------+


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
==================================

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
====================

Jobmon mainly relies on two infrastructure services in order to stay performant under heavy load, namely uWSGI and Kubernetes.

Kubernetes
==========

The main functionality of Kubernetes (k8s) is container orchestration.
The first step in deploying Jobmon is to build a Docker image for the Jobmon server code.
That image is then used to build a series of Docker containers, which are grouped into **pods**.
Each pod represents a subset of the server routes, see the above table.
For example, all /client/* routes are sent to the jobmon-client pod on Kubernetes.
Each pod is instantiated with 3 containers, each with a preset CPU/memory resource allocation.

The traefik router will try to equitably distribute incoming data between the associated containers.
For example, an incoming series of /client/* routes will be routed between each of the initial 3 client pods.
However, the load handled by the Jobmon service is not always equal. In the event of a very large workflow, or a series of concurrent workflows, the client side pods can get overwhelmed with incoming requests, leading to timeouts or lost jobs. Jobmon utilizes the Kubernetes `horizontal autoscaling algorithm <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`_ when it detects heavy load in the containers. As of 11/3/2020, "heavy load" is set `here <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/k8s/31_deployment_jobmon_client.yaml.j2#52-77>`_. Namely, when either CPU or memory is at 80% or more utilization, we can spin up more containers up to a limit of 10. The traefik router will then divert some incoming routes to the newly created containers in order to allow heavily-utilized containers to finish processes off. When the usage spike is over, and container usage dips below some minimum threshhold, the newly spawned containers will then be killed until we only have the three initial containers remaining.

All of the Kubernetes scaling behavior is configurable, per pod, in the associated .yaml files.

uWSGI
=====

uWSGI is a web service used to communicate between the client side application and the server code.
In our architecture, uWSGI runs inside each of the docker containers created by Kubernetes [#f1]_ .
uWSGI consists of a main process that manages a series of worker processes.


Like the Kubernetes deployment, each container starts with a minimum number of workers as specified `here <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/jobmon/server/deployment/container/uwsgi.ini#35>`_. If a specific container falls under heavy load, uWSGI can utilize a cheaper algorithm to spawn more workers and process the additional incoming requests. There are a variety of cheaper algorithms that can determine when to scale up/down worker processes - Jobmon uses the `busyness algorithm <https://uwsgi-docs.readthedocs.io/en/latest/Cheaper.html#busyness-cheaper-algorithm>`_. Under this specification, busyness is set by average utilization over a given time period. Configurations can be set in the same uwsgi.ini file linked above.

Similarly to the Kubernetes pod autoscaler, the busyness algorithm will create workers to handle a usage spike and spin down workers when usage is low. This is important for two reasons:

1. A container can efficiently process incoming requests with more workers. If there are no free workers to handle a request, it will sit in the queue until a worker frees up. If requests are incoming more quickly than the workers can execute, this can potentially result in long queue wait times and request timeouts.
2. Without worker autoscaling behavior the resource threshholds needed for Kubernetes horizontal autoscaling will not be reached. Remember that Kubernetes defines busyness by container CPU and memory usage. Adding workers directly adds to the CPU usage, and indirectly adds to memory usage by allowing more concurrent data flow. If the additional threads in the container cannot be allocated work due to lack of autoscaling, then the requisite busyness needed in each container won't be reached. Kubernetes does not track the length of the request queue as a busyness parameter.


Full stack demo
===============

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
======================

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