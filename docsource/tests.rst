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
of six jobs.

The main load test is three_phase_load_test.py.

If we want to check the spike caused by many jobs being created at once then
the simpler one_phase_load_test.py is adequate.

