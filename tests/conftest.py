import pytest

from jobmon.job_state_manager import JobStateManager


@pytest.fixture(scope='module')
def dag_id():
    jsm = JobStateManager()
    rc, dag_id = jsm.add_job_dag('test_dag', 'test_user')
    return dag_id
