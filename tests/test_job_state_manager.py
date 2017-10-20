import pytest

from jobmon.models import InvalidStateTransition


def test_jsm_valid_done(jsm_jqs, dag_id):
    jsm, jqs = jsm_jqs

    _, job_id = jsm.add_job("bar", "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_usage(job_instance_id, usage_str='used resources',
                  wallclock='00:00:00', maxvmem='1g', cpu='00:00:00', io='1')
    jsm.log_done(job_instance_id)


def test_jsm_valid_error(jsm_jqs):
    jsm, jqs = jsm_jqs

    _, dag_id = jsm.add_job_dag("mocks", "pytest user")
    _, job_id = jsm.add_job("bar", "baz", dag_id)
    jsm.queue_job(job_id)

    _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
    jsm.log_executor_id(job_instance_id, 12345)
    jsm.log_running(job_instance_id)
    jsm.log_error(job_instance_id, "this is an error message")


def test_invalid_transition(jsm_jqs):
    jsm, jqs = jsm_jqs

    _, dag_id = jsm.add_job_dag("mocks", "pytest user")
    _, job_id = jsm.add_job("bar", "baz", dag_id)

    with pytest.raises(InvalidStateTransition):
        _, job_instance_id = jsm.add_job_instance(job_id, 'dummy_exec')
