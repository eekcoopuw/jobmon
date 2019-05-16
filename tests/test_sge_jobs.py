from datetime import datetime, timedelta
from functools import partial

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from tests.timeout_and_skip import timeout_and_skip


def test_valid_command(real_dag_id, job_list_manager_sge):
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="sge_valid_command", num_cores=2, mem_free='4G',
             max_runtime_seconds='1000',
             j_resource=True,
             max_attempts=1))
    job_list_manager_sge.queue_job(job)

    # max_qw is the number of times it is allowed to be in qw state. Don't
    # set it to be 1, that is is too tight.
    timeout_and_skip(step_size=10, max_time=120, max_qw=3,
                     job_name='sge_valid_command',
                     partial_test_function=partial(
                         valid_command_check,
                         job_list_manager_sge=job_list_manager_sge))


def valid_command_check(job_list_manager_sge):
    job_list_manager_sge._sync()
    if len(job_list_manager_sge.all_done) == 1:
        # Success
        return True
    else:
        return False


def test_context_args(db_cfg, real_jsm_jqs, job_list_manager_sge):
    # Use the "-a" flag on qsub so that the job will only be eligible
    # for execution in the future, in this case 5 hours in the future.
    delay_to = (datetime.now() + timedelta(hours=5)).strftime("%m%d%H%M")
    job = job_list_manager_sge.bind_task(
        Task(command=sge.true_path("tests/shellfiles/jmtest.sh"),
             name="test_context_args", slots=2, mem_free='4G', max_attempts=1,
             max_runtime_seconds='1000',
             context_args={'sge_add_args': '-a {}'.format(delay_to)}))
    job_list_manager_sge.queue_job(job)

    # The job should not run so only one qw is needed
    timeout_and_skip(step_size=10, max_time=30, max_qw=1,
                     job_name="test_context_args",
                     partial_test_function=partial(
                         context_args_check,
                         db_cfg=db_cfg,
                         job_id=job.job_id))


def context_args_check(db_cfg, job_id):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        jis = DB.session.query(JobInstance).filter_by(job_id=job_id).all()
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
