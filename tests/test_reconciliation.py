import pytest

from jobmon.job_instance_factory import execute_batch_dummy
from jobmon.job_list_manager import JobListManager


@pytest.fixture(scope='function')
def job_list_manager_sge(dag_id):
    jlm = JobListManager(dag_id, executor=execute_batch_dummy)
    yield jlm
    jlm.disconnect()


def test_reconciler(job_list_manager_sge):
    job_id = job_list_manager_sge.create_job("ls", "sgefbb")
    job_list_manager_sge.queue_job(job_id)
    job_list_manager_sge.job_inst_factory.instantiate_queued_jobs()

    jir = job_list_manager_sge.job_inst_reconciler
    exec_ids = jir._get_presumed_instantiated_or_running()
    assert len(exec_ids) == 1
    assert exec_ids == 12
