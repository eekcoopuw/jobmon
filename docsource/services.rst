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


Troubleshooting a running JobStateManager and JobQueryServer
************************************************************

The "jobmon server" is actually three separate docker containers, executing on jobmon-p01. For emu update 3 they are:

1. jobmonemup3_jqs_1  (jobmon query service, emu update 3)
2. jobmonemup3_jsm_1  (jobmon state manager, emu update 3)
3. jobmonemup3_db_1  (jobmon database, emu update 3)

1. ssh to jobmon-p01
2. "docker ps" will show the running containers and their uptimes, for example see below.
3. Logs (voluminous):  "docker logs --tail 10 jobmonemup3_jsm_1"
4. To start a specific service use the name, eg:  "docker start jobmonemup3_jqs_1"
5. A container can be restarted by container id or name, eg "docker restart 4594e55149456" or "docker restart jobmonemup3_jqs_1"


.. image:: images/docker_ps.png

Deploying JobStateManager and JobQueryServer
********************************************

To deploy a centralized JobStateManager and JobQueryServer:

1. Ssh into jobmon.p01.ihme.washington.edu
2. cd into ~/tmp
3. Clone the jobmon repo into a new folder within ~/tmp, with a descriptive folder name::

    git clone ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git new_name

4. As per the "Version Control" section below, update the port numbers in, unless this has already been done:
  a. docker-compose.yaml
  b. this documentation
  c. the default .jobmonrc file
  d. jobmon/config.py
  e. jobmon/bootstrap.py
  f. k8s/db-service.yaml
  g. wait-for-db.sh
  h. wait-for-tables.sh
  i. And do a recurive grep to be sure!   e.g.   ``grep -r 3312 *``
5. Submit the new version number files back to git
6. From the root directory of the repo, run::

    ./runserver.py

You'll be prompted for a slack bot token.
Use the 'BotUserOathToken' from::

  https://api.slack.com/apps/AA4BZNQH1/install-on-team

Press the Copy button on the 'Bot User OAuth Access Token' text box.
The runserver.py script will not echo that Token when you paste it into the window because the python code is using the gepass input function.
The runserver.py script will also ask for two slack channels. There is a bug - you have to re-enter the default slack channel names, surrounded by single quotes.
The script will run docker-compose up build


Version Control
***************

For testing purposes, you can then access the jobmon database on that server
from your favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-p01.ihme.washington.edu
    port: 3313
    user: docker
    pass: docker


Each new version of jobmon increments the ports, so for example:

======== ==== ===== ===== ====
Version  jqs  jsm-1 jsm-2 db
======== ==== ===== ===== ====
emu.0    na   4556  4557  3307
emu.1    4658 4656  4657  3308
emu.2    4758 4756  4757  3309
emu.3    4858 4856  4857  3310
emu.3    4958 4956  4957  3311
emu.4    5058 5056  5057  3312
emu.5    5158 5156  5157  3313
======== ==== ===== ===== ====

The port numbers come in pairs, e.g. "3313:3306".
The number on the right of the colon is the port-number inside the container, and never changes.
The port number on the left of the colon is the external port number and must be changed on each release.
See also::
https://docs.docker.com/compose/networking/


Deployment architecture
***********************
.. image:: images/deployment_architecture.png

