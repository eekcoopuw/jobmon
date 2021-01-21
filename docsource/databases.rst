Database Deployments
####################

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
  + INNODB_BUFFER_POOL_SIZE +  80% of RA                 +
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

