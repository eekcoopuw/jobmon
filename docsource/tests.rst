Testing Strategy
################

The goal is that the pytest unit tests should have sufficient coverage that it
is safe to release if they are all green. That is not the case as of April 2019.
Bugs reached production even though the tests were green. Therefore every
production bug must have a new test written, and all new code must start with
a list of test cases.

Pytest is used for automatic tests, both unit tests and integration tests.

Load tests are manual and are performed using a deployment of the release
candidate. Therefore these are also User Acceptance Test (UAT).

Unit Tests
**********

These are "traditional" pytest unit tests. Some test simple modules, but most
actually test whole subsytems and are technically integration tests.
They do not use mocks because more bugs are found in lower levels if
mocks are not used. Mocks are more effort for less testing.
The exception is if you have to mock in order to change the behavior of the
remote system, typically to force it to pretend to be in error.

The fixtures for the unit tests, as defined in conftest.py::

           ephemera_conn_str
           Creates the ephemera database
           returns the database connection string
                   |

           test_session_config
           returns the database connection string
                   |
         +--------------------------------+----------------------+
         |                                |                      |
      create_dbs_if_needed             env_var              jsm_jqs
      If the database tables           Sets env vars        creates local flask
      do not exist then create it      to show in test           |
         |                                |                 no_request_jsm_jqs
         |                             local_flask_app      patches real server
         |                                |                 to use local flask
         +--------------------------------+
         |
      db_cfg
      Creates flask and the attached
      sqlalchemy database connection
         |
      real_jsm_jqs
      Creates the external jsm/jqs services
         |
      real_dag_id
      Creates a dag in the external service
      Returns the dag
         |
      job_list_manager_sge
      Creates another job list manager locally with faster times,
      less annoying to test

test_and_skip is an important function that protects against cluster instability.
Many tests create a SGE job and wait for it to complete. If the cluster is
busy then the job gets stuck in qw (queue-wait) mode. This function has a while
loop with a timeout. In essence it will skip the test if the job under test is
stuck in qw mode when the timeout occurs. So the test won't fail.

If a test needs to use a SQL UPDATE command to change a job, then make sure you
commit after every update. Subsequent operations are not guaranteed to get the
same database connection so they won't necessarily see the changes unless they
are committed.

*General Principle:* Report flakey tests and fix them.

Deployment Tests
****************

The deployment tests can be run manually after a deployment.

six_job_test.py is a simple little smoke test that runs a small application
of six jobs. It should be used to confirm that communication between the client, services, and DB are configured properly. If it fails that indicates the services are not properly configured.

Load Tests
**********

Load testing is a heuristic used to confirm that jobmon is hitting the performace benchmarks required to run large applications on IHME's cluster. Load testing is not covered by standard unit testing. It is not automated and requires a human participant.

The general principle is run a fake application on a fresh deployment of jobmon which mimics how a large application would interface with jobmon in order to confirm that jobmon can handle the load.

Things to check
^^^^^^^^^^^^^^^

 1) Run htop on the jobmon VM to confirm that cpu utilization doesn't get uncomfortably high while the application is running
 2) Run uwsgitop inside the jobmon service to make sure that work is evenly distributed accross workers. Also check that requests per second is a reasonable value (suggested: 150rps as of 0.9.3). To launch uwsgitop, log into the the VM and run ``docker exec -it jobmon{v}_jobmon_1 bash``. Once you are inside the jobmon service container, pip install uswgitop if it isn't already installed. To launch uwsgitop run ``uwsgitop /tmp/statsock``
 3) Analyze the server logs using the scripts found in '/homes/svcscicompci/scripts' to confirm that no queries are taking longer than ~.1 seconds or returning a payload that is too large (need a number here).
 4) Cluster configuration can affect job failure rate and we want to know when the cluster configuration has changed. Check the db to make sure that jobs aren't mysteriously, periodically failing.

The main load test is three_phase_load_test.py.

If we want to check the spike caused by many jobs being created at once then
the simpler one_phase_load_test.py is adequate.

Other load tests such as load_test_intermittent_exceptions.py can be used to load test error and retry routes.

Most of all, if something seems fishy, investigate the smell.
