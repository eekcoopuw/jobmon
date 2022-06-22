***********
Deployments
***********

TODO: UPDATE THIS PAGE
**********************

Database
########
Beginning with 1.1, each major release has had one database that is shared between all the dot releases.
For example, 1.1.0 through 1.1.5 all share the same database. 2.0.0 through 2.0.4 all share the
same database.

Prior to 1.1 each dot release usually had its own database.
From 2.0 onwards, the database is on a separate VM from the services.
The mysql database runs in a docker container on that VM,
in fact the only docker container on that host.
Therefore it can be configured to use 80% of the memory for its buffers, and use all of the threads.

The data is mounted on a persistent storage volume, mounted to that container.
It is persistent and therefore is not deleted when the container is stopped, or if the images
are pruned.

Critical Database Config Values
*******************************
Check these manually after a new database is created, or after a database reboot

  +-------------------------+----------------------------+
  + Setting                 +  Value                     +
  +=========================+============================+
  + INNODB_BUFFER_POOL_SIZE +  80% of RAM                +
  +-------------------------+----------------------------+
  + WAIT_TIMEOUT            +  600                       +
  +-------------------------+----------------------------+
  + THREAD_POOL_SIZE        +  Set automatically on boot +
  +-------------------------+----------------------------+



Using mysqldump to copy a database
**********************************

On the cluster, run a command like the following:
::
mysqldump -h jobmon-p01.ihme.washington.edu --port 3305 -u docker -p docker --database docker > dbs_3305_dump.sql


Spinning down a database
************************

1. SSH into the host machine, and use "docker ps" to find the container name that corresponds to the database to be spun down.
2. Used "docker stop <container_id>" to stop the container.

Enabling Slow Query Logging
***************************

This negatively affects performance, so only do it in produciton if there really is a problem.
It should be enabled for at least some of the time during load tests prior to a release.
You need to be logged in to mysql as the root user.

.. code-block:: sql

  SET GLOBAL slow_query_log = 'ON';
  SET GLOBAL long_query_time = 2; # in seconds, default is 10
  SET GLOBAL slow_query_log_file = '/path/filename'; # Default /var/log/mysql/mariadb-slow.log

Very Important: You have to log out and in for it to have an effect. You can check by:

.. code-block:: sql

  SHOW VARIABLES;

Turn it off when you are done:

.. code-block:: sql

  SET GLOBAL slow_query_log = 'OFF';


Archiving a volume-based database
*********************************

For versions <=1.0.3.

Refer to db_archiving/create_archive_db.sh for instructions


Removing a deprecated database
******************************

1. Copy the database to a backup location.
2. Spin down the database container.
3. Use "docker inspect -f '{{ .Mounts }}' <container_id>" to find the volume associated with the database. It is usually the first attribute of the value in the first element of the list. For example, volume "jobmon081_mysql-jobmon-emu":

    [{jobmon081_mysql-jobmon-emu /var/lib/docker/volumes/jobmon081_mysql-jobmon-emu/_data /var/lib/mysql local z true rprivate}]

Make sure the database has been copied/backed up before doing the next two steps!

4. Run "docker rm <container_id>" to permanently remove the container from the host machine.
5. Run "docker volume rm <volume_name>" to permanently remove the volume from the host machine.


Historical Port and Host Versions
*********************************
Can be found on the hub at https://hub.ihme.washington.edu/display/DataScience/Jobmon+Version+Record

Docker
######
Basic Steps
***********
1. Clone the jobmon repository
2. Make sure you have docker installed on your local machine
3. From the root of the jobmon directory, run::

    docker-compose -f deployment/docker-compose/docker-compose.yml.local_db_client_services --env-file deployment/docker-compose/.jobmon.ini up -d

4. Once the containers are up and running you can get into the client container to begin running your code by running::

    docker exec -it client bash

5. Once inside the container you can navigate to the /run directory ::

    cd ../run/

6. Where you will see your mounted file system. From there you can run your desired workflow script for example (if the jobmon directory is in the root of your mounted filepath)::

    python jobmon/deployment/tests/local_deploy_workflow_test.py --num 3

7. When you are done running your workflows, stop and remove the containers, remove the images, volume and prune the network


Further Configuration
*********************
Set machine specific configuration in the /deployment/docker-compose/jobmon.ini file

- WEB_SERVICE_PORT: external port to connect to flask services on (default connection is localhost:3000)
- EXTERNAL_DB_PORT: external db port to connect to db on (default is localhost:3306 user: read-only pass: docker)
- LOCAL_PATH: Filepath to mount on the container, default is your root directory: ~/.

How to Run a Workflow Locally
*****************************
The only special configuration for running locally is that you must set the
`executor_class` as either `SequentialExecutor` or `MultiprocessingExecutor` in your Workflow
Object and Task Objects

See the Quickstart docs to get started creating a workflow with tasks to run.

To Access the Local Database
****************************
In a standard sql database management application such as SequelPro or MySQL Workbench, connect to your database at::

    host: 0.0.0.0
    username: read_only
    password: docker
    port: 3306

The jobmon tables will be in the docker database

Azure
#####

Deploying on Azure Kubernetes Service (AKS)
===========================================

Intro
----------------------

This documentation describes an experimental deployment of Jobmon on AKS in July 2018.
This is not production code; the Guppy version of Jobmon is now deployed using Kubernetes
so many of the ideas in this document are probably obsolete. However, the document
will stay here until Jobmon is ported to Azure.

This describes deploying Jobmon on AKS, it is not Jobmon controlling jobs on Azure Batch.
Most of the work in the upcoming port of Jobmon to Azure will be creating an Azure Batch
executor.

This is a commentary on the following tutorials, that will walk you through the intricacies of
those tutorials within our ecosystem:
- https://docs.microsoft.com/en-us/azure/aks/,
- https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-macos?view=azure-cli-latest
- https://docs.microsoft.com/en-us/azure/aks/tutorial-kubernetes-prepare-app,
- https://docs.microsoft.com/en-us/azure/aks/tutorial-kubernetes-prepare-acr,
- https://docs.microsoft.com/en-us/azure/container-registry/container-registry-auth-aks


"Hello World" on AKS
----------------------

The first step is to check your Azure credentials and familiarize yourself with the Azure portal (http://portal.azure.com).

To work on Azure you need to use your **sadm account.** If your usual UW account is ``jcitizen`` then the sadm account
will be ``sadm_jcitizen``.
The sadm account is a separate account with a separate password, although the sadm account only exists because you already
have a UW account. The sadm account is connected to IHME billing via the Subscription ID.
Sadm accounts are created by the infrastructure team. If you don't have one you will need to ticket them, and explain
wy IHME should pay for what you are doing :-)

The AKS tutorial is quite good, although there are two gotchas as explained below (names can only have 63 characters
and should not have underscores or hyphens,
and be sure to upgrade the version of kubernetes).

We recommend you run all your az commands using az cli on your local computer, rather than the Azure command line on the Azure portal.

Tutorial Steps
~~~~~~~~~~~~~~

To avoid confusion with your usual UW account, it it safest to work in a private browser window,
or a browser that you don't normally use.

Open https://portal.azure.com, and enter your sadm account name, including the domain, e.g. ``jcitizen@uw.edu``.
Azure will redirect you to a UW login page.

Login to UW using your sadm account, NOT your usual UW account.
Azure will redirect you back to portal.azure.com, where you will now be logged in. See screenshot:

.. image:: images/azure_desktop.png

Click on your account widget in top right to check that:

  a. You are logged in as your sadm_account, and

  b. That account's subscription is assigned to SADM_BBRITT in the "My permissions" tab

.. image:: images/azure_permissions.png

In a different browser window, open the AKS tutorial, https://docs.microsoft.com/en-us/azure/aks/
Follow the Azure CLI instructions and **execute all instructions on your local computer, using az commands**. We recommend not using the Azure Portal instructions. The latter is a GUI that
only differs in the opening sequence,
but I noticed some obvious typos in the Azure Portal instructions.

On MacOS you will need to install Azure-Cli via homebrew; see
https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-macos?view=azure-cli-latest
If you haven’t set up a subscription you will be blocked at this step.
All az commands in the tutorial should run perfectly fine locally in a bash shell,
because the ``az`` commands have the azure portal address baked-in.

Now follow the tutorial to create a resource group and an AKS cluster. There are a few issues, so see the gotchas section. Most importantly, however:

**DO NOT use underscores or hyphens in any name, neither for resource group nor cluster.**
These characters are disallowed in some circumstances, allowed in others. For sanity it is best to be consistent –
ItsCamelCaseForUsNow.  Names appear to be case insensitive, which you can see by running ``az aks list``.

Create a resource group as per the tutorial. A resource group is essentially a namespace that owns all the other things you create.

Create the AKS Cluster as per the tutorial, this command can take ten minutes to run.
The command will display “running” messages until finally it returns a JSON object.
Notice that the Microsoft ``az aks`` commands **manage** kubernetes clusters on Azure Kernel Services.
These commands all need to know the name of the cluster (via ``--name/-n``),
and the resource group to which it belongs (via ``--resource-group/-g``).
Your AKS account can have multiple clusters (more on this below).
There is a useful summary of ``az aks`` commands here: https://docs.microsoft.com/en-us/cli/azure/aks?view=azure-cli-latest

You will need to install kubectl on your laptop. kubectl controls the cluster(s) that you create. **kubectl implicitly operates on the last cluster for which you downloaded credentials.**
This state is stored in ``.kube/config and your .ssh`` key directory.

**IMPORTANT:** Upgrade the version of kubernetes immediately.
Run the command ``az aks get-upgrades`` to discover if there is an upgrade version,
and then if necessary run ``az aks upgrade``, passing in the version with the ``-k`` flag.
An upgrade will take tens of minutes to run. If you don't upgrade then ``kubectl get nodes`` will
consistently fail with an "unknown error."
An example upgrade:  ``az aks upgrade -g myResourceGroup -n myAKSCluster --kubernetes-version 1.10.3``

Download the cluster credentials as per the tutorial.
That will modify your .ssh directory and .kube/config file.
If you delete a cluster and start a new one then it is probably safest to delete the existing .ssh directory before getting
the credentials to the new cluster.
You should receive a message similar to ``Merged "myAKSCluster" as current context in /home/geoffrey/.kube/config``

**THE CRITICAL TEST:** As per the tutorial, run ``kubectl get nodes``.  I was blocked here by an "unknown error."
I think the root cause were names-with-hyphens and mismatched kubernetes versions. It's okay if the node name output by ``kubectl get nodes`` does not exactly match your resource-group or cluster name.

If you want to deploy their example Flask voting app, continue with the tutorial to load the voting application. The final IP address is public and can be accessed by the whole world.
Therefore delete the service when your are done, using ``kubectl delete service azure-front-end``, similarly for the back-end service.

Creating Multiple Clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~

You can run the ``az aks create`` command with different cluster names and create multiple clusters.
Notice that they will have different kubernetes versions. Also notice how ``kubectl`` only operates on one
cluster at a time – the last one for which you downloaded the credentials. In relational speak:
*One AKS account to multiple clusters.*

For example, observe the switching between clusters ``myAKSCluster`` and ``mySecondCluster``:

.. image:: images/azure_multiple_clusters.png


Building and Deploying Voting App
-----------------------------------------------------------------------------

Note: This is just for deploying the Voting app. If you want to deploy jobmon, scroll down to the later applicable section.


For **Step 1**, we need to (once only) create a registry in AKS. The tutorial is good, although it has a few gotchas.
Most importantly, **run all the az commands run on your machine.**
The docker daemon must be on your machine, you cannot run a docker daemon in the cloud shell.
The ``az`` command set appears to be hard-wired internally to talk to azure.com, so it can run on any machine, it
does not need to be run in side the Azure cloud shell.
Start with **step 1**:

https://docs.microsoft.com/en-us/azure/aks/tutorial-kubernetes-prepare-app

The ``ak acr`` series of commands operate on container registries.
Change the animal names in the voting app so that you can be certain that it is your own code that is deployed:
``vim azure-vote/azure-vote/config_file.cfg``

Now, **step 2,** which also runs smoothly:

https://docs.microsoft.com/en-us/azure/aks/tutorial-kubernetes-prepare-acr

It is useful to define variables, e.g. for Azure Login Server:
``export ALS=$(az acr list --resource-group myResourceGroup --query "[].{acrLoginServer:loginServer}" --output tsv)``

Subsequently
``docker tag azure-vote-front $ALS/azure-vote-front:v1``

Try running various commands with ``--output table`` and ``--output tsv``. The first variant is good for humans,
the second is good for scripts.

**Step 3** did not run smoothly at first because our sadm accounts originally only had Contributor privileges,
not Owner privileges.
We now all have Owner privileges, but it would be better to work out how to avoid that.

Use ``az role assignment list`` to see roles of your account.
For example, I have two role assignments: Owner and Contributor. Same principal UUID, but two different roles::

    bash: az role assignment list --assignee sadm_gphipps@uw.edu
    {
        "additionalProperties": {},
        "canDelegate": null,
        "id": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/providers/Microsoft.Authorization/roleAssignments/eeecfc5c-e918-4d1d-a997-6eb12453383d",
        "name": "eeecfc5c-e918-4d1d-a997-6eb12453383d",
        "principalId": "486ec914-e7be-403b-8d3a-85ee9a1fc379",
        "principalName": "sadm_gphipps@uw.edu",
        "roleDefinitionId": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c",
        "roleDefinitionName": "Contributor",
        "scope": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2",
        "type": "Microsoft.Authorization/roleAssignments"
      },
      {
        "additionalProperties": {},
        "canDelegate": null,
        "id": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/providers/Microsoft.Authorization/roleAssignments/8cecb91d-501b-425d-b39a-3a7a7c68af57",
        "name": "8cecb91d-501b-425d-b39a-3a7a7c68af57",
        "principalId": "486ec914-e7be-403b-8d3a-85ee9a1fc379",
        "principalName": "sadm_gphipps@uw.edu",
        "roleDefinitionId": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/providers/Microsoft.Authorization/roleDefinitions/8e3af657-a8ff-443c-a75c-2fe8c4bcb635",
        "roleDefinitionName": "Owner",
        "scope": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2",
        "type": "Microsoft.Authorization/roleAssignments"
      },

If you leave off the ``--assignee`` flag it will show you the role assignments for all IHME sadm accounts.
Roles are described here:

https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles

You can use the portal to look at various objects. For example, navigate to ``Active Directory->App Registrations``
to see  application registrations.

For **Step 4 "Run Application"** I had to follow the advice given in "allow access via a Kubernetes secret," i.e.
https://docs.microsoft.com/en-us/azure/container-registry/container-registry-auth-aks

I copied the first shell script, changed the parameters to match my setup. Note that the tutorial only uses
one resource group, therefore ACR_RESOURCE_GROUP and AKS_RESOURCE_GROUP will be identical.

That script created a new service profile, which I could see because the client ID had changed.
I do not know why that script created a new application, because the only create command in the script
appears to create a new role assignment, not an application.
However, it is clearly meant to do so,as shown by an example near the bottom of this page:
https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-cli
I think the application creation was a side-effect of the scope that was used.

Be warned that names are not consistent in Azure,
an object that is an application in one command can be a client in another command.

This is the output from their script, which I saved locally as one.sh.
I modified the script to also print out the found client-id and acr-id::

    gphipps@D-10-19-204-251.dhcp4.washington.edu /Users/gphipps/hack/aks/mine: ./one.sh
    client-id ce3d4cfa-1d4e-42bd-b3ac-89ffedab744a
    acr-id /subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/resourceGroups/myResourceGroup/providers/Microsoft.ContainerRegistry/registries/mySecondRegistry
    {
      "canDelegate": null,
      "id": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/resourceGroups/myResourceGroup/providers/Microsoft.ContainerRegistry/registries/mySecondRegistry/providers/Microsoft.Authorization/roleAssignments/903d9639-3a8a-42be-920d-3357ba45a02a",
      "name": "903d9639-3a8a-42be-920d-3357ba45a02a",
      "principalId": "d6913702-dd19-4bdf-8268-a44de548dbe1",
      "resourceGroup": "myResourceGroup",
      "roleDefinitionId": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/providers/Microsoft.Authorization/roleDefinitions/acdd72a7-3385-48ef-bd42-f606fba81ae7",
      "scope": "/subscriptions/3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2/resourceGroups/myResourceGroup/providers/Microsoft.ContainerRegistry/registries/mySecondRegistry",
      "type": "Microsoft.Authorization/roleAssignments"
    }

Notice the restful structure, e.g. ``3bfb2d32-faa9-4d0d-bf95-fb8e32d9fbc2`` is our subscription ID.


Understanding Active Directory
------------------------------
Azure Active Directory is very complicated.

A tenant is an organization, sort of like a namespace. We appear to belong to a broad UW-IT tenant. See
https://docs.microsoft.com/en-us/azure/architecture/cloud-adoption-guide/adoption-intro/tenant-explainer

An Application is an entity in AD that holds all the security information for an actual application. A Service Principal
is an instance of that Application deployed from a particular Docker registry or home directory or tenant. See

https://docs.microsoft.com/en-us/azure/active-directory/develop/active-directory-how-applications-are-added

These quotes are illuminating:

    An Azure AD application is defined by its one and only application object,
    which resides in the Azure AD tenant where the application was registered, known as the application's "home" tenant.

    In order to access resources that are secured by an Azure AD tenant,
    the entity that requires access must be represented by a security principal.
    The security principal defines the access policy and permissions for the user/application in that tenant.

    Service principals are what govern an application connecting to Azure AD and can be considered the
    instance of the application in your directory.
    For any given application, it can have at most one application object (which is registered in a "home" directory)
    and one or more service principal objects representing instances of the application in every directory in which it acts.

Adding an application automatically creates one service principal.


Other Gotchas
~~~~~~~~~~~~~

If ``az aks list`` causes a traceback on a mac that refers to ``_cffi_backend`` then you need to follow instructions
on

https://github.com/Azure/azure-cli/issues/5034

The brew install failed on my mac because I needed to manually create some directories::
    sudo mkdir /usr/local/Frameworks
    sudo chmod 777 /usr/local/Frameworks/
    brew link --overwrite python3


Cleaning Up
-----------

Stopping a service
Find all service names:  ``kubectl get services``

``kubectl delete service azure-vote-front`` and ``kubectl delete azure-vote-back``

Deleting a cluster:
``az aks delete --resource-group myResourceGroup --name myAKSCluster``

Useful kubectl cheat sheet:

https://kubernetes.io/docs/reference/kubectl/cheatsheet/#deleting-resources

Removing an image from a registry:

``docker rmi azure-vote-front``


Deploying the Entire Jobmon Ecosystem on AKS
--------------------------------------------

This is an unscaled deployment of jobmon, i.e. with one deployment of each service.
In actual production we will scale to two (perhaps three) copies
of each service behind a load balancer so that we can do hot deploys.

The instructions are a merge of the instructions in k8s/readme.md (not yet merged into the master branch)

If you haven't created a AKS registry, then follow step 2 of the Building and Deploying Voting App section above.

Log in to the cluster you created on AKS from your local computer, in my case:

``az acr login --name mySecondRegistry``

Build the docker image for jobmon also from your local computer:

``cp jobmonrc-docker jobmonrc-docker-wsecrets
docker build -t jobmon .``

Now tag that image for your AKS repository.
``export ALS=$(az acr list --resource-group myResourceGroup --query "[].{acrLoginServer:loginServer}" --output tsv)``

and

``docker tag jobmon $ALS/jobmon``

In my case ALS is ``mysecondregistry.azurecr.io``

Now push (upload) the image to the AKS registry:

``docker push $ALS/jobmon``

We also need an image for mysql. AKS does not appear to be able to reach out and donwload a copy, so download one here,
tag it for AKS:

``docker pull mysql:5.6
docker tag mysql:5.6  $ALS/mysql:5.6
docker push $ALS/mysql:5.6``

Now update the kubernetes deployment yaml files to refer to images in the correct registries. The ``image`` tag needs
to be prepended by the registry name, e.g.::

  image: mysql:5.6
  image: jobmon

becomes::

  image: mysecondregistry.azurecr.io/mysql:5.6
  image: mysecondregistry.azurecr.io/jobmon

respectively.

Now start the jobmon cluster:

``kubectl apply -f k8s/
kubectl get services``

For example::

    gphipps@D-10-19-204-251.dhcp4.washington.edu /Users/gphipps/hack/jobmon: kubectl get services
    NAME         TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)                         AGE
    db           LoadBalancer   10.0.139.157   40.117.117.128   3312:31873/TCP                  18h
    jqs          LoadBalancer   10.0.75.86     40.117.112.109   5058:30869/TCP                  18h
    jsm          LoadBalancer   10.0.84.33     40.117.131.241   5056:32575/TCP,5057:31183/TCP   18h
    kubernetes   ClusterIP      10.0.0.1       <none>           443/TCP                         6d

If you need to debug then use kubectl to get logs. You need to know the pod names:

``kubectl get pods``

For example::

    bash: kubectl get pods
    NAME                       READY     STATUS      RESTARTS   AGE
    db-7b5b79768f-vs4cl        1/1       Running     0          1d
    initdb-5g4vk               0/1       Completed   0          1d
    jqs-6fc9bd58d5-hx6x5       1/1       Running     0          1d
    jsm-7cc69bfd97-lmthq       1/1       Running     0          1d
    monitor-7f97f697dc-gwkt9   1/1       Running     0          1d


And then

``kubectl logs jqs-6fc9bd58d5-hx6x5``

Shows the problem::

    bash: kubectl logs jqs-6fc9bd58d5-hx6x5
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping
    ERROR 2003 (HY000): Can't connect to MySQL server on 'db' (110 "Connection timed out")
     tables. DB is unavailable - sleeping

Scaling
-------

To be written. I tried it with hello world and it worked. The challenge with jobmon will be how to have one database
and multiple copies of jqs and jms. The services can all scale independently because they refer to each other by the
address of their front-ing load balancer, not by the IP addresses of the individual deployments.

Referring to the AKS tutorial here:

https://docs.microsoft.com/en-us/azure/aks/tutorial-kubernetes-scale

``az aks scale --resource-group=myResourceGroup --name=myFourthCluster --node-count 3``

As above, check the number of pods:
``kubectl get pods``

Output::

    bash: kubectl get pods
    NAME                       READY     STATUS      RESTARTS   AGE
    db-7b5b79768f-vs4cl        1/1       Running     0          1d
    initdb-5g4vk               0/1       Completed   0          1d
    jqs-6fc9bd58d5-hx6x5       1/1       Running     0          1d
    jsm-7cc69bfd97-lmthq       1/1       Running     0          1d
    monitor-7f97f697dc-gwkt9   1/1       Running     0          1d

Now scale just JQS:

``kubectl scale --replicas=3 deployment/jqs``

And checking with ``kubectl get pods``::

    bash: kubectl get pods
    NAME                       READY     STATUS              RESTARTS   AGE
    db-7b5b79768f-vs4cl        1/1       Running             0          1d
    initdb-5g4vk               0/1       Completed           0          1d
    jqs-6fc9bd58d5-5fp52       0/1       ContainerCreating   0          7s
    jqs-6fc9bd58d5-dpmw2       0/1       ContainerCreating   0          7s
    jqs-6fc9bd58d5-hx6x5       1/1       Running             0          1d
    jsm-7cc69bfd97-lmthq       1/1       Running             0          1d
    monitor-7f97f697dc-gwkt9   1/1       Running             0          1d


Hot Deployments
---------------

This should just work, see:

https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment

Monitoring
----------

To be written.

Kubernetes
##########
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

