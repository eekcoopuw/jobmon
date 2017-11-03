Deployed Services
#################

JobStateManager (JSM)
*********************

There is one central python process, the JobStateManager (JSM), that keeps
track of and forces consistency between individual Jobs and their JobInstances.
This process must be launched on a host and listen on ports known by all jobs
which will communicate with it.

The JSM ensures that when a JobInstance changes state the proper accounting
happens to its associated Job. For example, as soon as a JobInstance is
INSTANTIATED, the num_attempts property of its Job is incremented by 1. When
the JobInstance proceeds to RUNNING, the Job transitions to RUNNING in lock
step. Most importantly, the JSM keeps track of how many JobInstances have
been attempted per Job and advances the Job to an ERROR state should the
number of attempts exceed a user-defined threshold.

In order to persist this accounting, the JSM writes to a database. When
configuring the JSM process (typically via a ~/.jobmonrc file deployed to the
JSM host machine), the connection string should specify a user+password that
has INSERT and UPDATE privileges on the jobmon database.


JobQueryServer (JQS)
********************

While the JobStateManager handles updates to the Workflow state, the
JobQueryServer (JQS) handles requests for information about the Workflow state.
In other words, it handles read-requests on the jobmon database.  When
configuring the JQS process (typically via a ~/.jobmonrc file deployed to the
JQS host machine), the connection string should specify a user+password that
has SELECT privileges on the jobmon database, but does not require INSERT or
UPDATE privileges.


Deploying JobStateManager and JobQueryServer
********************************************

To deploy a centralized JobStateManager and JobQueryServer:

1. Login to jobmon-p01
2. Clone this repo into a folder called "jobmon_cavy"::

    git clone ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git jobmon_cavy

3. From the root directory of the repo, run::

    docker-compose up --build -d

That should do it. Now you'll just need to make sure your users have the proper
host and port settings in their ~/.jobmonrc::

    {
      "host": "jobmon-p01.ihme.washington.edu",
      "jsm_rep_port": 4456,
      "jsm_pub_port": 4457,
      "jqs_port": 4458
    }

For testing purposes, you can then access the jobmon database on that server
from your favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-p01.ihme.washington.edu
    port: 3306
    user: docker
    pass: docker


.. todo::

    Make these settings the default upon installing the package (or
    alternatively source jobmonrc from a shared location, then from the user's
    home directory).


Deployment architecture
***********************
.. image:: images/deployment_architecture.png
