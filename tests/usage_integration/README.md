# Testing the Usage Integrator

The tests in this subdirectory are not run as part of the standard Jobmon test suite. These tests are automatically 
deselected if you run `nox -s tests -- tests/` from the top level directory, as the integrator is considered relatively 
decoupled from core Jobmon. 

If you do want to develop on and test the integrator, you can do so as following: 
1. Write the connection parameters for the SLURM accounting database to a JSON file. Save this file to
`tests/usage_integration/integrator_secrets.json`
   1. This file should contain the following 5 keys: 
      1. `DB_HOST_SLURM_SDB`
      2. `DB_PORT_SLURM_SDB`
      3. `DB_NAME_SLURM_SDB` 
      4. `DB_USER_SLURM_SDB`
      5. `DB_PASS_SLURM_SDB`
   2. The corresponding values can be fetched from the Rancher secrets store, tagged as `jobmon-slurm-sdb-[dev/prod]`.
   3. This JSON file is added to .gitignore to avoid committing secrets to the source repository. If you wish to run 
   the tests on a different machine you'll have to re-write this JSON file.
2. Call `nox -s test_integrator -- tests/` from the top level directory.
   1. Don't use the -n flag to enable multiprocessing. The UsageQ object operates as a singleton, so running multiple 
   tests in parallel is likely to cause data concurrency issues and erroneously break tests. 

The integrator is not considered fully tested until it is deployed on Kubernetes and a load test is run against the 
database it is configured to poll. Once the unit tests all pass, ensure that it is operational in K8s by following:
1. Build a Python wheel of the newly updated integrator, and push to Pypi. There are 2 ways to do so:
   1. Building a distribution is done by the `jobmon_pr_merged` Jenkins pipeline, so create and merge a PR to kick off 
   that project. You can PR to a staging branch if you want to avoid merging prior to full PR review. 
   2. Build and deploy manually. You can create a wheel by running `nox -s build` from the top level directory, followed
   by `twine upload --repository-url https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared --username svc_artifactory_scicomp --password <> ./dist/*`
      1. The password can be looked up in Jenkins
2. Run the deploy_integrator Jenkins pipeline. Point this build to a development database
3. (if necessary) Deploy an instance of the jobmon server to point to the same development database.
4. Log onto the SLURM cluster
5. Install jobmon and jobmon-slurm into a conda environment, and run `jobmon_config update` to point to the appropriate
web service.
6. Run a six job test or a load test, and monitor the integrator logs to ensure that all completed tasks have the
correct resources reported. If so, you can declare the work done and open up a pull request.