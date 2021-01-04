Local Docker Deployment
=======================

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