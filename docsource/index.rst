.. jobmon documentation master file, created by
   sphinx-quickstart on Fri Sep 23 09:01:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

The Job Monitor (jobmon)
=========================================================

Launching a monitor
===================

To launch a monitoring server where jobs can log their statuses, you'll have to
clone the feature/dockerized_mysql branch of the `jobmon repository
<https://stash.ihme.washington.edu/projects/CC/repos/jobmon>`_ and use the
included Dockerfile and docker-compose.yml. Before building the 'jobmon' docker
image, you'll need to replace *DB_USERNAME* and *DB_PASSWORD* in
docker-compose.yml with valid credentials for a user with read/write privileges
on internal-db-p02.ihme.washington.edu/jobmon. With your new passwords in
place, you can go ahead and build an image tagged "jobmon" and launch using
compose. The process should look something like this::

    git clone ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git
    cd jobmon
    git checkout feature/dockerized_mysql
    sed -ie 's/DB_USERNAME/my_valid_db_username/g' docker-compose.yml
    sed -ie 's/DB_PASSWORD/my_valid_db_password/g' docker-compose.yml
    docker build -t jobmon .
    docker-compose up -d --force-recreate

Those commands should leave you with two freshly running containers:
*jobmon_monitor_1* and *jobmon_db_1*. By default, the monitor will listen on
port 5555 for status messages from Jobs and JobInstances. You can modify that
port in the docker-compose.yml file.


Contents:

.. toctree::
   :maxdepth: 2

          Home <index>
          overview
          Module reference <modules>



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
