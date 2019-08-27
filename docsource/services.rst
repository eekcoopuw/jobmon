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


Spinning down a database
************************

1. SSH into the host machine, and use "docker ps" to find the container name that corresponds to the database to be spun down.
2. Used "docker stop <container_id>" to stop the container.


Removing a deprecated database
******************************

1. Copy the database to a backup location.
2. Spin down the database container.
3. Use "docker inspect -f '{{ .Mounts }}' <container_id>" to find the volume associated with the database. It is usually the first attribute of the value in the first element of the list. For example, volume "jobmon081_mysql-jobmon-emu":

    [{jobmon081_mysql-jobmon-emu /var/lib/docker/volumes/jobmon081_mysql-jobmon-emu/_data /var/lib/mysql local z true rprivate}]

Make sure the database has been copied/backed up before doing the next two steps!

4. Run "docker rm <container_id>" to permanently remove the container from the host machine.
5. Run "docker volume rm <volume_name>" to permanently remove the volume from the host machine.


Database Locations
******************

====================================== ========================= ====================== =============
Docker Volume Name                     Container Name            Host                   External Port
====================================== ========================= ====================== =============
db3311_jobmon060again_mysql-jobmon-emu jobmon-60-3311-archive    jobmon-archive-db-p01  3311
forkedjobmon_mysql-jobmon-emu          jobmon-forked-db-archive  jobmon-archive-db-p01  3312
jobmon_mysql-jobmon-emu                jobmon-unknown-db-archive jobmon-archive-db-p01  3765
jobmon060again_mysql-jobmon-emu        jobmon-60-archive         jobmon-archive-db-p01  3766
jobmon063again_mysql-jobmon-emu        jobmon-63-archive         jobmon-archive-db-p01  3305
jobmon067_mysql-jobmon-emu             jobmon-67-archive         jobmon-archive-db-p01  3314
jobmon071_mysql-jobmon-emu             jobmon071_db_1            jobmon-archive-db-p01  3316
jobmon072_mysql-jobmon-emu             jobmon072_db_1            jobmon-archive-db-p01  3317
jobmon080_mysql-jobmon-emu             jobmon-80-db-archive      jobmon-archive-db-p01  3800
jobmon081_mysql-jobmon-emu             jobmon-81-db-archive      jobmon-archive-db-p01  3810
jobmon083_mysql-jobmon-emu             jobmon-83-db-archive      jobmon-archive-db-p01  3830
jobmon3313_mysql-jobmon-emu            jobmon-3313-db-archive    jobmon-archive-db-p01  3313
jobmonemup3_mysql-jobmon-emu           jobmon-emup3-db-archive   jobmon-archive-db-p01  3310
jobmonnov2018_mysql-jobmon-emu         jobmon-nov2018-db-archive jobmon-archive-db-p01  3767
jobmon089_mysql-jobmon-emu             jobmon089_db_1            None                   3890
jobmon090_mysql-jobmon-emu             jobmon090_db_1            None                   3900
jobmon095_mysql-jobmon-emu             jobmon095_db_1            jobmon-docker-cont-p01 3950
jobmon098_mysql-jobmon-emu             jobmon099_db_1            jobmon-archive-db-p01  3390
jobmon100_mysql-jobmon-emu             jobmon100_db_1            jobmon-archive-db-p01  10000
jobmon101_mysql-jobmon-emu             jobmon101_db_1            jobmon-docker-cont-p01 10010
jobmon102_mysql-jobmon-emu             jobmon102_db_1            jobmon-docker-cont-p01 10020
jobmon_mysql-jobmon-emu                jobmon0001postdev58_db_1  jobmon-p01             10030
====================================== ========================= ====================== =============

Note: jobmon089_db_1 and jobmon090_db_1 were lost due to the June 10 2019 UW Tower Outage

Accessing a Database
********************

For testing purposes, you can access the jobmon database on that server
from your favorite DB browser (e.g. Sequel Pro) using the credentials::

    host: jobmon-p01.ihme.washington.edu
    port: 10030
    user: read_only
    pass: docker

    or host: jobmon-docker-cont-p01.hosts.ihme.washington.edu (depending on the version)


Version Naming System
*********************

As of version 1.0.0, Jobmon follows the `Semantic Versioning 2.0.0`_ naming system:

    Given a version number MAJOR.MINOR.PATCH, increment the:

    1. MAJOR version when you make incompatible API changes,
    2. MINOR version when you add functionality in a backwards-compatible manner, and
    3. PATCH version when you make backwards-compatible bug fixes.

Because ports are tied directly to the version number, versions will increment regardless of the rules above if a PATCH release increments past 99 or if a MINOR release increments past 9.

.. _Semantic Versioning 2.0.0: https://semver.org/

Port Assignment Convention
**************************

As of version 1.0.0, Jobmon ports reflect MAJOR, MINOR, (if a server side change is made) PATCH, and its service (DBs, JQS, etc.).

======================== ======
Service                  Number
======================== ======
Database                 0
JQS/JSM/JVS              1
======================== ======

For example:

    Given port ABCDE, A = MAJOR, B = MINOR, CD = PATCH, and E = service and release 1.0.12, ports would be the following:

    1. Database: 10120
    2. JQS/JSM/JVS: 10121

If a client-side only PATCH (1.0.13) was released, the release would still refer to the above ports.

Jobmon Version Record
*********************

jobmon-p01.ihme.washington.edu

======== ==== ===== ===== ==== ============= ================
Version  jqs  jsm-1 jsm-2 db   git-tag       dbs-notes
======== ==== ===== ===== ==== ============= ================
emu.0    na   4556  4557  3307
emu.1    4658 4656  4657  3308
emu.2    4758 4756  4757  3309
emu.3    4858 4856  4857  3310
emu.3    4958 4956  4957  3311 0.6.0
emu.4    5058 5056  5057  3312 0.6.1
emu.5    4458 4456  4457  3305 kelly-and-leo  063again
emu.6    5158 5156  5157  3313 0.6.6          Database-lost
emu.7    5258 5256  5257  3314 0.6.7
http     6258 6256  n/a   3315 0.7.0
http.2   6258 6256  n/a   3316 release-0.7.1
http.3   7258 7256  n/a   3317 release-0.7.2
http.4        8256        3800 release-0.8.0
http.5        8356        3810 release-0.8.1
http.6        8356        3820 release-0.8.2
http.7        8456        3830 release-0.8.3
======== ==== ===== ===== ==== ============= ================

jobmon-docker-cont-p01.hosts.ihme.washington.edu

========  ==== ===== ===== =====  =============
Version   jqs  jsm-1 jsm-2 db     git-tag
========  ==== ===== ===== =====  =============
http.8         8457        3840   release-0.8.4
http.9         8458        3841   release-0.8.5
http.10        8656        3860   release-0.8.6
http.11        8756        3870   release-0.8.7
http.12        8856        3880   release-0.8.8
http.13        8956        3890   release-0.8.9
http.14        9056        3900   release-0.9.0
http.15        9056        3900   release-0.9.1
http.16        9056        3900   release-0.9.2
http.17        9056        3900   release-0.9.3
http.18        9056        3900   release-0.9.4
http.19        9556        3950   release-0.9.5
http.20        9556        3950   release-0.9.6
http.21        9556        3950   release-0.9.7
http.22        9856        3980   release-0.9.8
http.23        10001       10000  release-1.0.0
http.24        10011       10010  release-1.0.1
http.25        10021       10020  release-1.0.2
http.26        10031       10030  release-1.0.3
========  ==== ===== ===== =====  =============

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

2. To check the pypi server to make sure that there is not an existing build of
the version you just tagged you can go to:
http://https://pypi.services.ihme.washington.edu/simple/jobmon/ to see if that the
version is not already present.

3. If the version exists, it can be override by setting the overide_package_on_pypi
Jenkins flag to true.

4. Run a jenkins build setting:

    repo_url: ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git

    branch: master (or any branch you want to build on)

    python_vers: 3

    skip_tests: True

    qlogin-fair: scicomp-uge-submit-p01-direct

    qlogin: qlogin

    test_mode: False (if set to True, it will use the provided tag; otherwise, it uses the tag in git.)

    existing_db: False (if set to True, please provide the server_user_password)

    slack_token: (our slack token)

    slack_api_url: https://slack.com/api/chat.postMessage

    wf_slack_channel: jobmon-alerts

    node_slack_channel: suspicious_nodes

    jobmon_server_hostname: jobmon-p01

    jobmon_server_sqdn: jobmon-p01.ihme.washington.edu

    jobmon_service_port: (port of this version)

    jobmon_version: 1.0.4 (this only takes effect when test_mode=True)

    reconciliation_interval: 30

    hearbeat_interval: 90

    report_by_buffer: 3.1

    internal_db_host: db

    internal_db_port: 3306

    external_db_host: jobmon-p01.ihme.washington.edu

    external_db_port: (db port of this version)

    jobmon_pass_service_user: (this only takes effect when test_mode=True)

    tag_prefix: release-

    overide_package_on_pypi: true

5. If the build completes successfully, check the docs again to
make sure the new version is up and labelled as expected


Deploying JobStateManager and JobQueryServer
********************************************

To deploy a centralized JobStateManager and JobQueryServer:

1. Make sure you have properly build and deployed to jenkins, then ssh into jobmon-p01.ihme.washington.edu using your svcscicompci ssh key::

    ssh -i ~/.ssh/svcsci_id_rsa svcscicompci@jobmon-p01.ihme.washington.edu

2. cd into ~/tmp
3. Clone the jobmon repo into a new folder within ~/tmp, with a descriptive folder name like jobmon-<version>::

    git clone ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git new_name

6. Activate the jobmon conda environment:
    source activate jobmon
7. Edit <jobmon root>/jobmon/jobmon.cfg

    a. Fill in the values in [basic values] session.

    b. If creating a new jobmon DB, fill in the values in [new db] session.

    c. If using an existing jobmon DB, fill in the values in [existing db] session.

       Note: to obtain the jobmon_pass_service_user, run::

                  docker exec  -it <container_id> /bin/bash

       then do a::

                  set

8. Under <jobmon root>/jobmon/server/deployment, run::

    python run_server.py

9. Once the server is up, you should see three containers: jobmon, monitor, and db. Vier <jobmon root>/.env to get DB password.

Pushing the Docker Image to the Registry
****************************************
If you use run_server.py, and set test_mode to False, the run_server script uploads the image for you.


Deployment architecture
***********************
.. image:: images/deployment_architecture.png

