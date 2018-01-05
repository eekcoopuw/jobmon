import pytest
import sys

from datetime import datetime, timedelta

from jobmon import sge
from jobmon.database import session_scope
from jobmon.models import JobInstance, JobInstanceStatus

from tests.timeout_and_skip import timeout_and_skip


if sys.version_info < (3, 0):
    from functools32 import partial
else:
    from functools import partial


def test_valid_command(dag_id, job_list_manager_sge):
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/jmtest.sh"),
        "sge_foobar", job_hash='somehash', slots=2, mem_free=4,
        project='proj_jenkins', max_attempts=3)
    job_list_manager_sge.queue_job(job_id)

    timeout_and_skip(10, 120, 1, partial(
        valid_command_check,
        job_list_manager_sge=job_list_manager_sge))


def valid_command_check(job_list_manager_sge):
    done = job_list_manager_sge.get_new_done()
    if len(done) == 1:
        # Success
        return True
    else:
        return False


def test_context_args(jsm_jqs, job_list_manager_sge):
    delay_to = (datetime.now() + timedelta(minutes=5)).strftime("%m%d%H%M")
    job_id = job_list_manager_sge.create_job(
        sge.true_path("tests/shellfiles/jmtest.sh"), "sge_foobar",
        job_hash='somehash', slots=2, mem_free=4, max_attempts=3,
        project='proj_jenkins',
        context_args={'sge_add_args': '-a {}'.format(delay_to)})
    job_list_manager_sge.queue_job(job_id)

    timeout_and_skip(10, 180, 1, partial(
        context_args_check,
        job_id=job_id))


def context_args_check(job_id):
    with session_scope() as session:
        jis = session.query(JobInstance).filter_by(job_id=job_id).all()
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
