import pytest

from jobmon.job_state_manager import JobStateManager
from jobmon.models import InvalidStateTransition


def test_jsm_valid_done():
    jsm = JobStateManager()

    _, dag_id = jsm.add_job_dag("foo", "pytest user")
    _, job_id = jsm.add_job("bar", "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id)
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_done(job_instance_id)


def test_jsm_valid_error():
    jsm = JobStateManager()

    _, dag_id = jsm.add_job_dag("foo", "pytest user")
    _, job_id = jsm.add_job("bar", "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id)
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_error(job_instance_id, "this is an error message")


def test_invalid_transition():
    jsm = JobStateManager()

    _, dag_id = jsm.add_job_dag("foo", "pytest user")
    _, job_id = jsm.add_job("bar", "baz", dag_id)

    with pytest.raises(InvalidStateTransition):
        _, job_instance_id = jsm.add_job_instance(job_id)
