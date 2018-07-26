Deploying on Azure Kubernetes Service (AKS)
===========================================

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

Search for "Kubernetes" in the search box, and select "Kubernetes Service."

In a different browser window, open the AKS tutorial, https://docs.microsoft.com/en-us/azure/aks/
Follow the Azure CLI instructions, not the Azure Portal instructions. The latter is a GUI that
only differs in the opening sequence,
but I noticed some obvious typos in the Azure Portal instructions.

As per the tutorial, open **cloud shell** in the header, bash variant (unless you like Powershell, but I suspect fewer
customers use it and therefore it will be less reliable).
If you haven’t set up a subscription you will be blocked at this step.
The cloud shell is a full bash shell with command history that persists between sessions.
This cloud shell window times out rapidly, so keep it will fed with carriage returns.

**DO NOT use underscores or hyphens in any name, neither for resource group nor cluster.**
These characters are disallowed in some circumstances, allowed in others. For sanity it is best to be consistent –
ItsCamelCaseForUsNow.  Names are appear to be case insensitive, which you can see by running ``az aks list``.

Create a resource group as per the tutorial.

Create the AKS Cluster as per the tutorial, this command can take ten minutes to run.
The command will display “running” messages until finally it returns a JSON object.
Notice that the Microsoft ``az aks`` commands **manage** kubernetes clusters on Azure Kernel Services.
These commands all need to know the name of the cluster (via ``--name/-n``),
and the resource group to which it belongs (via ``--resource-group/-g``).
Your AKS account can have multiple clusters (more on this below).
There is a useful summary of ``az aks`` commands here: https://docs.microsoft.com/en-us/cli/azure/aks?view=azure-cli-latest

You should not need to install kubectl, it was already installed for me.
kubectl controls the cluster(s) that you create. **kubectl implicitly
operatse on the last cluster for which you downloaded credentials.**
This state is stored in ``.kube/config` and your ``.ssh`` key directory.

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
I think the root cause were names-with-hyphens and mismatched kubernetes versions.

Continue with the tutorial to load the voting application. The final IP address is public and can be accessed by the whole world.
Therefore delete the service when your are done, using ``kubectl delete service azure-front-end``, similarly for the back-end service.

Creating Multiple Clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~

You can run the ``az aks create`` command with different cluster names and create multiple clusters.
Notice that they will have different kubernetes versions. Also notice how ``kubectl`` only operates on one
cluster at a time – the last one for which you downloaded the credentials. In relational speak:
*One AKS account to multiple clusters.*

For example, observe the switching between clusters ``myAKSCluster`` and ``mySecondCluster``:

.. image:: images/azure_multiple_clusters.png


Building and Deploying a service from our Source Code in our Stash Repository
----------------------------------------------------------------------------

To be done, a placeholder.


Deploying the Entire Jobmon Ecosystem on AKS
--------------------------------------------

To be done, a placeholder.