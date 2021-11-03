Kubernetes Deployment
#####################

Jobmon is deployed on Kubernetes (k8s), for two main reasons. Firstly, it takes advantage of Kubernetes to perform internal networking, so that the different deployment units and monitoring elements can communicate with minimal developer effort. Secondly, the Kubernetes control plane can flexibly scale up and down container instances as needed in order to be flexible with load.

Kubernetes deployments can communicate each other by creating internal services that link container ports, in order to enable data exchange. This is necessary for logging and workflow reaping, for example.


Architecture
************

.. image:: diagrams/k8s_architecture.svg

Since we often need to manage multiple versions of the Jobmon service at one time, the majority of deployment units are grouped together into a single **namespace**. In the above diagram, we have a sample Jobmon deployment, with two concurrent production versions of Jobmon running in separate namespaces. Within each namespace is also an Elastic monitoring stack, responsible for log aggregation and performance monitoring of the Jobmon service.

Inside a namespace, all internal services can reach each other via DNS lookup - Kubernetes assigns the DNS names automatically. External traffic, either external to Kubernetes entirely or from a separate namespace, is all routed through Traefik. Traefik can then route the incoming requests to the appropriate service.

The Jobmon reaper introduces some added complexity to the networking architecture outlined above, where there is 1 version of Jobmon/ELK per namespace. The reaper is dependent on the allowed/disallowed finite state machine transitions, so each version of Jobmon needs its own reaper in order to ensure that new or modified states are accounted for. However, server-side updates with no client-facing changes often are "hot deployed" so that users can take advantage of server upgrades without needing to upgrade their clients. While this is fine for the service as the Jobmon service is stateless, the reaper is not - it depends on database state, so old reapers cannot be spun down and reinstantiated like the service deployment can.

The solution is to move the reapers to a separate namespace. The jobmon-reapers namespace exposes one service per k8s namespace, and forwards web traffic to that namespace's Traefik controller. Then each reaper deployment can simply connect to the reaper service, ensuring that hot deploys and updates can be made to the target namespace without eliminating existing reaper deployments.

Helm
====

Jobmon's kubernetes deployments are managed by Helm, a self-described "package manager for Kubernetes". Rather than deploy individual deployments and services one by one, we can instead define Helm charts to spin up/down the major Jobmon components in the correct order.

Helm charts are deployed to a specific namespace, and can be upgraded or rolled back freely. Helm maintains up to ten versions of a deployed chart.

However, Helm cannot deploy objects to a namespace besides the target namespace. This can be problematic for certain resources, such as Traefik's custom resource definitions and RBACs that are defined in the Rancher global namespace. As a result, global resources still need to be deployed manually.

Additionally, Helm can't truly add or delete resources from a chart definition. For example, the jobmon reapers helm chart only defines a single reaper that is version-specific. When the reapers chart is upgraded in k8s, the existing reaper deployments are then **orphaned** from Helm, meaning that they are no longer managed by the Helm package manager (deletions, updates, etc. must now be done manually).

