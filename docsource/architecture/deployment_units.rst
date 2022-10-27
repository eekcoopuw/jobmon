
Deployment View
***************

*Which pieces of code are deployed where.*

Jobmon is deployed in three places:

- Client, in the same process as the Python control script
- Worker-node, a wrapper container around the actual UGE Task
- Server, as a set of Kubernetes services, defined below

Python Client
=============
This ia standard Python wheel that is pip-installable. At run-time the Python client is within the
Application's Python process and is just an ordinary library. It communicates via http to the
central kubernetes services.

*Strategies aka Executors*

The strategy package is part of the client. It represents the Cluster Operating system.
Jobmon has three at present:

- UGE (aka SGE)
- Sequential (one job after another), and
- Multiprocessing (jobs launched using Python MP)
- Dummy, which does nothing and is used to test Jobmon's internal machinery.

Only the UGE distributor is used in production, the others are useful for testing, and for
the upcoming Jobmon-on-a-laptop deployment.

Be very aware of the difference between where:

1. Where the Jobmon services are deployed (kubernetes or docker), and
2. Where the jobs that Jobmon controls are running.

**These are two separate axes:**

**(Kubernetes, Docker) CROSS (UGE, SLURM, Azure, Python-Sequential, Python-MP, Dummy)**

R-Client & Executor Service
===========================
As of January 2021 we are experimenting with an R-client that calls Python immediately
via the R reticulate package. Each Python API call has an R equivalent.
The Python interpreter runs in the same process as the R interpreter, so values are passed
directly in memory. The translation overhead is not known.

The second step will be to separate all the machinery that is currently in the Python client
into an ExecutorService that will contain the ``scheduler`` and ``strategies`` packages.
Python and R clients will simply use http to communicate with it when necessary. Calls from
the application that are currently synchronous (e.g. execute dag) will become asynchronous.
The executor service could be deployed locally (using Python MP), or deployed centrally as
a highly-scaled kubernetes container.

Worker-node
===========
The worker_node code is inside the Client package, it should move into its own package.

If Jobmon was only supporting UGE then the worker-node code could be moved
to a new top-level package, named worker-node.
However, Jobmon will control jobs on UGE, Azure, and SLURM in the near future, so it will
need a package structure.
UGE and Slurm can probably share the same execution_wrapper because they both run on Linux.
Azure needs a different execution wrapper.
What matters is the worker node operating environment (Linux vs docker), not the cluster OS.
Therefore this package will be moved as part of the port to Azure.

Server & Services
=================

The server package contains the kubernetes services, plus the model objects for communicating
to the mysql database.

As of 2.0 (Guppy) the Jobmon production server is deployed as a series of Kubernetes containers.
Prior to 1.0.3 Jobmon, services were deployed using docker. That docker capability will return
in 2.2 as the "Bootable on a Laptop" feature.

Each container is responsible for the routes from one external system or client.
The containers are organized according to the load they carry, so that they can scale independently:

+-------------------+-----------------------------------------------------+-------------------+
| Container/Package | Description and Comments                            | Domain Objects    |
+===================+=====================================================+===================+
| jobmon-client     | Handles requests from the the Python client inside  | Tool, Workflow    |
|                   | the application code, at bind time.                 | Task, Attributes  |
|                   | Therefore it creates workflows                      | TaskTemplate      |
|                   | and tasks. Basically a CRUD service.                |                   |
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
| workflow-reaper   | Continually check for lost & dead workflows         |    WorkflowRun    |
+-------------------+-----------------------------------------------------+-------------------+

.. The architecture diagrams are SVG, stored in separate files.
.. SVG is renderable in browsers, and can be edited in inkscape or on draw.io
.. image:: ../diagrams/deployment_and_message_flow.svg

*****************************
Clients From Developers Guide
*****************************

The Jobmon client is conceptually responsible for basic CRUD (Create, Read, Update, Delete). The client creates and modifies Jobmon
objects like Workflow, Tool, and Task in memory, and adds these objects to the database when the user requests a workflow run.

At the moment of writing, the client also currently includes logic for workflow execution and DAG traversal - however,
this logic will likely eventually be refactored into the web service.

Python Client
=============

The core client logic is written in Python, available to use from the ``jobmon`` Python package. A Python user can
install Jobmon into their conda/singularity/etc. environment, and write a control script defining their workflow. The user
will also need to install a plugin package, which acts as an execution interface into the desired cluster they wish to run
on. If installing via conda or using ``pip install jobmon[ihme]``, then all IHME-required plugin packages will be installed
automatically as well (UGE and SLURM).

R Client
========

R users, who may not have experience writing any Python but still wish to use Jobmon, can use the **jobmonr** library to
write their control scripts. All core logic is still written in Python, but the R client uses the **reticulate** package
to build a simple wrapper around the Python client. This allows a user to create and manipulate the necessary Jobmon
objects without needing to write any Python. The code for the R client does not live in this repository, but in the SCIC/jobmonr
repository.

Because there is no self-contained execution logic in the R client, users do need to take a dependency on a Python client.
This is done by setting the RETICULATE_PYTHON environment variable *prior* to loading the jobmonr library. This environment
variable should be a file path to the Python executable within a conda environment that has Jobmon installed, generally
ending with something like ``/miniconda3/bin/python``. If not set by a user, then the R client will default to a centrally
installed conda environment that's managed by the Scicomp team.

**Note**: A key limitation of any reticulate package is that you can only have 1 Python interpreter in memory. This means that
if you want to use another reticulate package in the same control script, you will need to install all dependencies needed for
both packages into a single conda environment. Additionally, both packages need to be flexible in allowing the user to set
the appropriate interpreter. For example, the widely-used (at IHME) crosswalk package, produced by MSCA, has a hardcoded, custom-built
conda environment hardcoded in the startup section, meaning that users *cannot* use both jobmonr and crosswalk in the same memory space.
However, there is very little reason to do so in the first place, and a small update to the crosswalk package could solve this issue.

Future Directions
=================

In the long term, the scope of the client should be restricted solely to creation of metadata to send to the database.
DAG traversal and scheduling/distributing would then be performed inside a long-running back end service container.

The decision to use reticulate in the R client was mainly motivated by wanting to avoid the challenge of rewriting all
of the distributor in R. Reticulate provides an extremely simple reference-based interface into a Python program, but
comes with its own set of challenges surrounding type conversions and environment management. If the client is simplified
into pure metadata creation, then we could replace the reticulate interface with a S6-class and httr based approach.


Kubernetes
==========

Kubernetes (k8s) provides container orchestration.
The first step in deploying Jobmon is to build a Docker image for the Jobmon server code.
That image is then used to build a series of Docker containers, which are grouped into **pods**.
Each pod represents a subset of the server routes, see the above table.
For example, all /client/* routes are sent to the jobmon-client pod on Kubernetes.
Each pod is instantiated with 3 containers, each with a preset CPU/memory resource allocation.


.. image:: ../diagrams/k8s_architecture.svg

Since we often need to manage multiple versions of the Jobmon service at one time,
the majority of deployment units are grouped together into a single **namespace**.
In the above diagram, we have a sample Jobmon deployment,
with two concurrent production versions of Jobmon running in separate namespaces.
Within each namespace is also an Elastic monitoring stack,
responsible for log aggregation and performance monitoring of the Jobmon service.

Inside a namespace, all internal services can reach each other via DNS lookup -
Kubernetes assigns the DNS names automatically.
External traffic, either external to Kubernetes entirely or
from a separate namespace, is all routed through Traefik.
Traefik can then route the incoming requests to the appropriate service.

The Jobmon reaper introduces some added complexity to the
networking architecture outlined above, where there is one version of
Jobmon/ELK per namespace. The reaper is dependent on the allowed/disallowed
finite state machine transitions, so each version of Jobmon needs its own reaper
in order to ensure that new or modified states are accounted for.
However, server-side updates with no client-facing changes often are "hot deployed" so that users can take advantage of server upgrades without needing to upgrade their clients. While this is fine for the service as the Jobmon service is stateless, the reaper is not - it depends on database state, so old reapers cannot be spun down and reinstantiated like the service deployment can.

The solution is to move the reapers to a separate namespace.
The jobmon-reapers namespace exposes one service per k8s namespace, and
forwards web traffic to that namespace's Traefik controller.
Then each reaper deployment can simply connect to the reaper service,
ensuring that hot deploys and updates can be made to the target namespace
without eliminating existing reaper deployments.


Metallb
=======

Metallb is the load balancer that comes packaged with Kubernetes.
It is only used to provide the Virtual IP (VIP) to the clients; it does not actually do any
load balancing.

Traefik
=======
Traefik (pronounced *tray-fick*) is an open-source edge router, which means that it parses the
incoming URL and routes the message to the appropriate back-end service.
It also loads balances across the set of kubernetes instances for a service.
For example, an incoming series of /client/* routes will be routed between each of the initial 3 client pods.
However, the load handled by the Jobmon service is not always equal.
In the event of a very large workflow, or a series of concurrent workflows,
the client-side pods can get overwhelmed with incoming requests, leading to timeouts or lost jobs.
Jobmon utilizes the Kubernetes
`horizontal autoscaling algorithm <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`_
when it detects heavy memory or CPU load in the containers.
As of 11/3/2020, "heavy load" is set in `31_deployment_jobmon_client.yaml.j2 <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/k8s/31_deployment_jobmon_client.yaml.j2#52-77>`__.
Namely, when either CPU or memory is at 80% or more utilization,
we can spin up more containers up to a limit of 10
The Traefik router will then divert some incoming routes to the newly created containers in order
to allow heavily-utilized containers to finish processes off. When the usage spike is over,
and container usage dips below some minimum threshold,
the newly spawned containers will then be killed until we only have the
three initial containers remaining.


Autoscaling Behavior
====================

Jobmon mainly relies uWSGI and Kubernetes to autoscale so as to remain performant under
heavy load. The database is also tuned to use all threads on its VM, and
80% of the available memory for its buffers.

uWSGI
=====

uWSGI is a web service used to communicate between the client side application and the server code.
In our architecture, uWSGI runs inside each of the docker containers created by Kubernetes [#f1]_ .
uWSGI consists of a main process that manages a series of flask worker processes.

Like the Kubernetes deployment, each container starts with a minimum number of workers
as specified `here <https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon/browse/jobmon/server/deployment/container/uwsgi.ini#35>`_. If a specific container falls under heavy load, uWSGI can utilize a cheaper algorithm to spawn more workers and process the additional incoming requests. There are a variety of cheaper algorithms that can determine when to scale up/down worker processes - Jobmon uses the `busyness algorithm <https://uwsgi-docs.readthedocs.io/en/latest/Cheaper.html#busyness-cheaper-algorithm>`__. Under this specification, busyness is set by average utilization over a given time period. Configurations can be set in the same uwsgi.ini file linked above.
See the configuration in ``deployment/config/app/uwsgi.ini``

Similarly to the Kubernetes pod autoscaler, the busyness algorithm will create workers to
handle a usage spike and spin down workers when usage is low. This is important for two reasons:

1. A container can efficiently process incoming requests with more workers. If there are no free workers to handle a request, it will sit in the queue until a worker frees up. If requests are incoming more quickly than the workers can execute, this can potentially result in long queue wait times and request timeouts.
2. Without worker autoscaling behavior the resource thresholds needed for Kubernetes horizontal autoscaling will not be reached. Remember that Kubernetes defines busyness by container CPU and memory usage. Adding workers directly adds to the CPU usage, and indirectly adds to memory usage by allowing more concurrent data flow. If the additional threads in the container cannot be allocated work due to lack of autoscaling, then the requisite busyness needed in each container won't be reached. Kubernetes does not track the length of the request queue as a busyness parameter.

UWSGI is configured to restart workers after a certain number of requests or seconds have
passed. This guards against memory leaks.


Full stack demo
===============

Take a simple Jobmon request: we want to manually set the state of a workflow run to be state "E", so the workflow can be retried.

``wfr.update_status("E")``

1. The update_status function constructs the **/swarm/workflow_run/<workflow_run_id>/update_status** route, which is processed by flask on the client side.
2. Flask sends the request to the Kubernetes service
3. The traefik controller routes the request to the swarm pod, then to a "free" container within the pod.

  a. If all containers are at high capacity, a new container is created.

4. uWSGI, running inside the container, assigns resources to handle the request.

  a. The main process either assigns a worker to the request, or instantiates a new worker process to handle the request.

5. The requested Python/SQL logic is executed within the worker process, and the returned data is sent back to the main process.
6. The main process sends the returned data back to the client application.


Performance Monitoring
======================

The Kubernetes cluster workload metrics can be tracked on `Rancher <https://k8s.ihme.washington.edu/c/c-99499/monitoring>`_. Regarding autoscaling, the important information to track is the per-pod container workload metrics. The container-specific workloads can be seen by navigating to the jobmon cluster -> namespace (dev or prod) -> pod (client, swarm, visualization, etc.).

The **Workload Metrics** tab displays a variety of time series plots, notably CPU Utilization and Memory Utilization, broken down by container. This allows tracking of what resources are running in each container. When evaluating performance during heavy load, it's important to check the utilization metrics to ensure containers are using the right amount of resources. Low utilization means container resources are not being used efficiently, and high utilization means the autoscaler is not behaving properly.

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
