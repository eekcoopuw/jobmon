import sys

from datetime import datetime, timedelta

from jobmon import sge
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.client.workflow.executable_task import ExecutableTask as Task

from tests.timeout_and_skip import timeout_and_skip


if sys.version_info < (3, 0):
    from functools32 import partial
else:
    from functools import partial


def test_valid_command(real_dag_id, job_list_manager_sge):
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_foobar", slots=2, mem_free=4, max_attempts=3))
    job_list_manager_sge.queue_job(job)

    timeout_and_skip(10, 120, 1, partial(
        valid_command_check,
        job_list_manager_sge=job_list_manager_sge))


def valid_command_check(job_list_manager_sge):
    job_list_manager_sge._sync()
    if len(job_list_manager_sge.all_done) == 1:
        # Success
        return True
    else:
        return False


def test_context_args(real_jsm_jqs, job_list_manager_sge):
    delay_to = (datetime.now() + timedelta(minutes=5)).strftime("%m%d%H%M")
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_foobar", slots=2, mem_free=4, max_attempts=3,
             context_args={'sge_add_args': '-a {}'.format(delay_to)}))
    job_list_manager_sge.queue_job(job)

    timeout_and_skip(10, 180, 1, partial(
        context_args_check,
        job_id=job.job_id))


def context_args_check(job_id):
    from jobmon.database import ScopedSession
    jis = ScopedSession.query(JobInstance).filter_by(job_id=job_id).all()
    njis = len(jis)
    status = jis[0].status
    sge_jid = jis[0].executor_id
    # Make sure the job actually got to SGE
    if njis == 1:
        # Make sure it hasn't advanced to running (i.e. the -a argument worked)
        assert status == JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR
        # Cleanup
        sge.qdel(sge_jid)
        return True
    else:
        return False
