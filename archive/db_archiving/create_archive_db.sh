#!/usr/bin/env bash

# Archiving a volume-based Jobmon database to a bind-mount based archive:
# 0. Decommission the Jobmon service you wish to archive.
#        Decommissioning means:
#           - Users have been notified and given time to migrate off the version in question.
#           - All of the associated containers EXCEPT for the database have been stopped.
# 1. Export/mysqldump the whole database through SequelPro (gear icon on bottom left) and save the file to your local storage. This can take a while.
#       - Make sure all of the database tables have been selected! Leave the other options as default.
# 2. SSH into the archive host and run create_archive_db.sh
# 3. Through SequelPro, connect to the archive database. Credentials are root/docker.
# 4. In SequelPro's query tab, run "CREATE DATABASE docker;" and "USE docker;"
# 5. Through SequelPro, select File > Import, then select the .sql file from step 1 and click confirm. This can take a while.
# 6. Stop the original database container.

# Once Jobmon no longer use volumes and they've all been archived, we can remove this file.


# About the script:

# Brings up a blank database in a container with a bind mount in a given directory

# example usage: make_jobmon_archive_db.sh jobmon-100-db-archive db_100 10000

# logical_name: name of the container
# mount: absolute path to the directory where container files will be kept on the archive host
# port: external port from which the database will be visible from the archive host

logical_name=$1
mount=$2
port=$3

# aka directory to put in 'source' field for bind mount in command below
source_dir="/Database/jobmon_db_archive/$mount"

# docker requires an existing directory - it won't make one for us.
mkdir $source_dir

docker run --name $logical_name --mount type=bind,source=$source_dir,target=/var/lib/mysql -e MYSQL_ROOT_PASSWORD=docker -p $port:3306 mysql:5.7 &