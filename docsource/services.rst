Deployed Services
#################

Jobmon is deployed as docker containers, hosted on a VM provided by the infra
team. We manage the docker containers directly, not via Rancher.
That VM/docker-daemon host multiple versions of Jobmon simultaneously.
Different versions of jobmon are either referred to by their release number
or database port number. The latter is not unique, because an update to the client
that does not change the server database will not rev the database port.

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
configuring the JQS process
the connection string should specify a user+password that
has SELECT privileges on the jobmon database, but does not require INSERT or
UPDATE privileges.

Jobmon Monitoring Service
*************************
This watches for disappearing dags  and workflows, as well as failing nodes.
It reports failing nodes to a specific slack channel.


Troubleshooting a running JobStateManager and JobQueryServer
************************************************************

The "jobmon server" is actually three or four separate docker containers,
executing on jobmon-p01.
From release 0.8.0 and onwards there are two docker containers:

1. jobmon082_monitor_1  (monitoring service)
2. jobmon082_jobmon_1  (jobmon query service and jobmon state manager)
3. jobmon082_db_1  (jobmon mysql database, release 0.8.2)

Prior to 0.8.0 there were four containers, the JQS qnd JSM were separated :

1. jobmon071_monitor_1  (monitoring service)
2. jobmon071_jqs_1  (jobmon query service, emu update 3)
3. jobmon071_jsm_1  (jobmon state manager, emu update 3)
4. jobmon071_db_1  (jobmon database, emu update 3)

1. ssh to jobmon-p01
2. "docker ps" will show the running containers and their uptimes, for example see below.
3. Logs (voluminous):  "docker logs --tail 10 jobmon082_jobmon_1"
4. To start a specific service use the name, eg:  "docker start jobmon082_jobmon_1"
5. A container can be restarted by container id or name, eg "docker restart 4594e55149456" or "docker restart jobmon082_jobmon_1"


.. image:: images/docker_ps.png


Using mysqldump to copy a database
**********************************

On the cluster, run a command like the following:

  mysqldump -h jobmon-p01.ihme.washington.edu --port 3305 -u docker -p docker --database docker  > dbs_3305_dump.sql


Version Control
***************

For testing purposes, you can then access the jobmon database on that server
from your favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-p01.ihme.washington.edu
    port: 3830
    user: read_only
    pass: docker

    or host: jobmon-docker-cont-p01.hosts.ihme.washington.edu (depending on the version)


Each new version of jobmon increments the ports and the db port reflects the
release number (0.8.0 = 3800), so for example:

jobmon-p01
========  ==== ===== ===== ==== ============= ==========
Version   jqs  jsm-1 jsm-2 db   git-tag       dbs-notes
========  ==== ===== ===== ==== ============= ==========
emu.0     na   4556  4557  3307
emu.1     4658 4656  4657  3308
emu.2     4758 4756  4757  3309
emu.3     4858 4856  4857  3310
emu.3     4958 4956  4957  3311  0.6.0
emu.4     5058 5056  5057  3312  0.6.1
emu.5     4458 4456  4457  3305  kelly-and-leo  063again
emu.6     5158 5156  5157  3313  0.6.6          Database-lost
emu.7     5258 5256  5257  3314  0.6.7
http      6258 6256  n/a   3315  0.7.0
http.2    6258 6256  n/a   3316  release-0.7.1
http.3    7258 7256  n/a   3317  release-0.7.2
http.4         8256        3800  release-0.8.0
http.5         8356        3810  release-0.8.1
http.6         8356        3820  release-0.8.2
http.7         8456        3830  release-0.8.3
========  ==== ===== ===== ==== ============== =======


jobmon-docker-cont-p01
========  ==== ===== ===== ==== =============
Version   jqs  jsm-1 jsm-2 db   git-tag
========  ==== ===== ===== ==== =============
http.8         8457        3840 release-0.8.4
http.9         8458        3841 release-0.8.5
http.10        8656        3860 release-0.8.6
http.11        8756        3870 release-0.8.7
========  ==== ===== ===== ==== =============

The port numbers come in pairs, e.g. "3313:3306".
The number on the right of the colon is the port-number inside the container, and never changes.
The port number on the left of the colon is the external port number and must be changed on each release.
See also::
https://docs.docker.com/compose/networking/

Note that Docker does "NATing" (Network Address Translation) so that the
mysql database is listening on port 3306 within its contained, but docker
maps it to a different port externally.


Updates before a new version can be deployed
********************************************
If your most recent commit on master is ready to be deployed, make sure that
the ports have been updated for the new version:

1. To update the ports, make a PR with the port numbers incremented according
to the version control [above] in the following places:

  a. docsource/services.rst
  b. docsource/quickstart.rst
  c. jobmon/models/attributes/constants.py
  d. And do a recursive grep to be sure!   e.g.   ``grep -r 3800 *``

2. Check that the correct host and password information is available in
quickstart.rst and this (services.rst)

Creating a Jenkins build to deploy your new version to the PyPi server
**********************************************************************
1. Tag the most recent commit (that contains updated ports) on stash with the
version that you are going to deploy, tag with the format release-0.8.4 and
make sure that you can see the tag in the stash UI (sometimes tagging through
command line doesn't show up and work properly)

  a. You can tag directly through the stash UI by clicking on the commit and
  adding a tag

2. Check the pypi server to make sure that there is not an existing build of
the version you just tagged you can go to:
http://dev-tomflem.ihme.washington.edu/docs/jobmon/ to make sure that the
version is not already present

3. If there is already a version deployed with the same version either:

  a. Tag with a new version that doesn't exist (best if you haven't already
  set up matching port numbers and deployed the database accordingly), and
  build as normal

  b. Go on to the pypi server (pypi.services.ihme.washington.edu) with your
  normal ihme credentials and delete the tar for the given build in the pypi
  docker container (this should be your last resort)

  c. If you had to delete the version from the pypi server, you need to edit
  the jenkins file to rebuild even if it has built that version before,
  the easiest way to do this is by clicking replay on a previously successful
  build that ran without tests, editing the jenkinsfile to make sure that when
  it deploys to the server it doesn't first check if version exists, and
  running that.

4. If you are sure that the version doesn't exist, run a jenkins build setting
skip_tests=True. If the build completes successfully, check the docs again to
make sure the new version is up and labelled as expected


Deploying JobStateManager and JobQueryServer
********************************************

To deploy a centralized JobStateManager and JobQueryServer:

1. Make sure you have properly build and deployed to jenkins, then ssh intojobmon-p01.ihme.washington.edu using your svcscicompci ssh key::

    ssh -i ~/.ssh/svcsci_id_rsa svcscicompci@jobmon-p01.ihme.washington.edu

2. cd into ~/tmp
3. Clone the jobmon repo into a new folder within ~/tmp, with a descriptive folder name like jobmon-<version>::

    git clone ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git new_name

6. Activate the jobmon conda environment:
    source activate jobmon
7. From the root directory of the repo, run::

    ./runserver.py

    Note: By the end of Mar 7, 2019, on jobmon-docker-cont-p01, the version of docker-compose comes with the conda environment has a bug, but the downgrade is blocked by other packages, so a working version has been put under ~/bin.
          Do `export PATH="~/bin:$PATH"` to use the bypass version.

You'll be prompted for a slack bot token.
Use the 'BotUserOathToken' from::

  https://api.slack.com/apps/AA4BZNQH1/install-on-team

Press the Copy button on the 'Bot User OAuth Access Token' text box.
The runserver.py script will not echo that Token when you paste it into the window because the python code is using the getpass input function.
The runserver.py script will also ask for two slack channels.
There is a bug - you have to re-enter the default slack channel names, surrounded by single quotes.
The script will run ``docker-compose up build``

Notice that the most priviliged database passwords are randomly generated in runserver.py
They are then set as environment variables in the docker service container. To
see them, connect to the docker container like this:
``docker exec -it jobmon071_jqs_1 bash``
and do a `env`, look for: ``DB_USER & DB_PASS``

Deployment architecture
***********************
.. image:: images/deployment_architecture.png

