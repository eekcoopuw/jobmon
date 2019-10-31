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

Steps to Deploy Services
************************
Can be found on the hub at https://hub.ihme.washington.edu/display/DataScience/Jobmon+Deployment

Deployment architecture
***********************
.. image:: images/deployment_architecture.png

