import logging
from datetime import datetime, timedelta
from functools import partial

import os
import pytest

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.executable_task import ExecutableTask as Task
from jobmon.client.utils import _run_remote_command
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_instance_status import JobInstanceStatus
from tests.conftest import teardown_db
from tests.timeout_and_skip import timeout_and_skip
from tests.conftest import teardown_db

path_to_file = os.path.dirname(__file__)


def test_valid_command(real_dag_id, jlm_sge_daemon):
    job = jlm_sge_daemon.bind_task(
        Task(command=sge.true_path(f"{path_to_file}/shellfiles/jmtest.sh"),
             name="sge_valid_command", num_cores=2, m_mem_free='4G',
             max_runtime_seconds=1000,
             j_resource=True,
             max_attempts=1))
    jlm_sge_daemon.adjust_resources_and_queue(job)

    def valid_command_check(job_list_manager_sge):
        job_list_manager_sge._sync()
        if len(job_list_manager_sge.all_done) == 1:
            # Success
            return True
        else:
            return False

    # max_qw is the number of times it is allowed to be in qw state. Don't
    # set it to be 1, that is is too tight.
    timeout_and_skip(step_size=10, max_time=120, max_qw=3,
                     job_name='sge_valid_command',
                     partial_test_function=partial(
                         valid_command_check,
                         job_list_manager_sge=jlm_sge_daemon))



def test_context_args(db_cfg, jlm_sge_daemon):
    # Use the "-a" flag on qsub so that the job will only be eligible
    # for execution in the future, in this case 5 hours in the future.
    delay_to = (datetime.now() + timedelta(hours=5)).strftime("%m%d%H%M")
    job = jlm_sge_daemon.bind_task(
        Task(command=sge.true_path(f"{path_to_file}/shellfiles/jmtest.sh"),
             name="test_context_args", num_cores=2, m_mem_free='4G',
             max_attempts=1,
             max_runtime_seconds=1000,
             context_args={'sge_add_args': '-a {}'.format(delay_to)}))
    jlm_sge_daemon.adjust_resources_and_queue(job)

    def context_args_check(db_cfg, job_id) -> bool:
        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            jis = DB.session.query(JobInstance).filter_by(job_id=job_id).all()
            njis = len(jis)
            status = jis[0].status
            sge_jid = jis[0].executor_id
        # Make sure the job actually got to SGE
        if njis == 1:
            # Make sure it hasn't advanced to running (the -a argument worked)
            assert status == JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR
            # Cleanup
            sge.qdel(sge_jid)
            return True
        else:
            return False

    # The job should not run so only one qw is needed
    timeout_and_skip(step_size=10, max_time=30, max_qw=1,
                     job_name="test_context_args",
                     partial_test_function=partial(
                         context_args_check,
                         db_cfg=db_cfg,
                         job_id=job.job_id))
    teardown_db(db_cfg)


@pytest.mark.skip("Fails too often, needs a new approach")
def test_intel_args_positive(db_cfg, job_list_manager_sge):
    # Positive test - we want Intel
    teardown_db(db_cfg)
    architecture_specific_args(db_cfg,
                               job_list_manager_sge,
                               test_name="test_intel_arg",
                               architecture_name="intel",
                               cluster_architecture_name="GenuineIntel",
                               )
    teardown_db(db_cfg)


@pytest.mark.skip("Fails too often, needs a new approach")
def test_intel_args_negative(db_cfg, job_list_manager_sge):
    teardown_db(db_cfg)
    # Negative test - we don't want Intel
    teardown_db(db_cfg)
    architecture_specific_args(db_cfg,
                               job_list_manager_sge,
                               test_name="test_intel_arg",
                               architecture_name="intel",
                               cluster_architecture_name="AuthenticAMD",
                               yes_or_no=False
                               )
    teardown_db(db_cfg)


@pytest.mark.skip("Fails too often, needs a new approach")
def test_amd_args_positive(db_cfg, job_list_manager_sge):
    teardown_db(db_cfg)
    architecture_specific_args(db_cfg,
                               job_list_manager_sge,
                               test_name="test_amd_arg",
                               architecture_name="amd",
                               cluster_architecture_name="AuthenticAMD"
                               )
    teardown_db(db_cfg)


@pytest.mark.skip("Fails too often, needs a new approach")
def test_amd_args_negative(db_cfg, job_list_manager_sge):
    teardown_db(db_cfg)
    architecture_specific_args(db_cfg,
                               job_list_manager_sge,
                               test_name="test_amd_arg",
                               architecture_name="amd",
                               cluster_architecture_name="GenuineIntel",
                               yes_or_no=False
                               )
    teardown_db(db_cfg)


def architecture_specific_args(db_cfg,
                               job_list_manager_sge,
                               test_name: str,
                               architecture_name: str,
                               cluster_architecture_name: str,
                               yes_or_no=True
                               ):
    if yes_or_no:
        suffix = ""
    else:
        suffix = "=FALSE"

    job = job_list_manager_sge.bind_task(
        Task(command="sleep 5",
             name=test_name,
             num_cores=1, m_mem_free='1G',
             max_attempts=1,
             max_runtime_seconds='100',
             context_args={
                 'sge_add_args': f"-l {architecture_name}{suffix}"}))
    job_list_manager_sge.adjust_resources_and_queue(job)

    timeout_and_skip(step_size=10, max_time=30, max_qw=1,
                     job_name=test_name,
                     partial_test_function=partial(
                         node_architecture_check,
                         db_cfg=db_cfg,
                         job_id=job.job_id,
                         cluster_architecture_name=cluster_architecture_name
                     ))


def node_architecture_check(db_cfg, job_id: int,
                            cluster_architecture_name: str) -> bool:
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    node_name = "no-name"
    with app.app_context():
        jis = DB.session.query(JobInstance).filter_by(job_id=job_id).all()
        status = jis[0].status
        node_name = jis[0].nodename
        print(f">>>>>> {status} on {node_name}")
    if status != "D":
        return False
    else:
        command = "python {script}". \
            format(script=sge.true_path(f"{path_to_file}/get_cpu_vendor_name.py"))

        exit_code, stdout_str, stderr_str = _run_remote_command(node_name,
                                                                command)
        print(f"Comp {command}: {exit_code}, {stdout_str}")
        return exit_code == 0 and cluster_architecture_name in stdout_str
