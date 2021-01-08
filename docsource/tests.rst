Testing Strategy
################

Jobmon has two sets of tests – automatic unit tests and manual integration/load tests.

A unit test tests one component, usually with the other components below it.

An integration test (also known as an end-to-end test) tests the system from the outside,
from the point of view of a user or an external system. Jobmon has three types of integration
tests:
1. Smoke Tests
#. Longevity Tests
#. Load Tests

A smoke test is a quick test for overall system functionality.
A longevity test is similar to a smoke test but it is run for days, with many calls,
typically searching for race conditions,
memory leaks, or other rare errors or errors caused by a build-up in resource utilization.
A Load Test is used to find the scaling limits of a release.

Some smoke tests are automatic, e.g. test_simple_dag. These tests were created using a
Test Driven Development strategy early in Jobmon's history.
Other smoke tests are purely manual. The Soak and Load Tests are run
manually, following a dev or prod deployment.

The goal is that the pytest unit tests should have sufficient coverage that it
is safe to release if they are all green.

Thee unit tests use the ephe,mera in memory database, so that they each have a clean database.

Unit Tests
**********

These are "traditional" pytest unit tests. Some test simple modules, but most
actually test whole subsytems and are technically integration tests.
They do not use mocks because more bugs are found in lower levels if
mocks are not used. Mocks are more effort for less testing.
The exception is if you have to mock in order to change the behavior of the
remote system, typically to force it to pretend to be in error.

The fixtures for the unit tests are defined in conftest.py::

           ephemera (& boot_db)
           Creates one instance of the ephemera database
           returns the database connection string
                   |
         +--------------------------------------+
         |                                      |
      web_server_process                      db_cfg
        Creates all services in flask        Creates all services in flask
        in a separate process.               in the same process.
        Not used outside of conftest.        *Frequently used outside of conftest*
         |
      client_env
        exports FQDN and ports
        *Frequently used outside of conftest*

If a test needs to use a SQL UPDATE command to change a job, then make sure you
commit after every update. Subsequent operations are not guaranteed to get the
same database connection so they won't necessarily see the changes unless they
are committed.

*General Principle:* Report flakey tests and fix them.

Deployment Tests
****************

The deployment tests must be run manually after a deployment.

six_job_test.py is a simple little smoke test that runs a small application
of six jobs. It should be used to confirm that communication between the client, services,
and DB are configured properly.
If it fails that indicates the services are not properly configured.

TODO
Define which tests to be run, with typical parameters

Load Tests
**********

Load testing is a heuristic used to confirm that jobmon is hitting the performance benchmarks
required to run large applications on IHME's cluster.
Load testing is not covered by standard unit testing.
It is not automated and requires a human participant.

The general principle is run a fake application on a fresh deployment of jobmon which mimics how a large application would interface with jobmon in order to confirm that jobmon can handle the load.

Things to check
^^^^^^^^^^^^^^^

*TODO This section needs to be updated for the kubernetes deployment.*

 1) Run htop on the jobmon VM to confirm that cpu utilization doesn't get uncomfortably high while the application is running
 2) Run uwsgitop inside the jobmon service to make sure that work is evenly distributed accross workers. Also check that requests per second is a reasonable value (suggested: 150rps as of 0.9.3). To launch uwsgitop, log into the the VM and run ``docker exec -it jobmon{v}_jobmon_1 bash``. Once you are inside the jobmon service container, pip install uswgitop if it isn't already installed. To launch uwsgitop run ``uwsgitop /tmp/statsock``
 3) Analyze the server logs using the scripts found in '/homes/svcscicompci/scripts' to confirm that no queries are taking longer than ~.1 seconds or returning a payload that is too large (need a number here).
 4) Cluster configuration can affect job failure rate and we want to know when the cluster configuration has changed. Check the db to make sure that jobs aren't mysteriously, periodically failing.

The main load test is three_phase_load_test.py.

If we want to check the spike caused by many jobs being created at once then
the simpler one_phase_load_test.py is adequate.

Other load tests such as load_test_intermittent_exceptions.py can be used to load test error and retry routes.

Most of all, if something seems fishy, investigate the smell.
