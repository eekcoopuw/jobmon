import logging
from datetime import datetime, timedelta
from functools import partial

import os
import pytest

from jobmon.client.execution.strategies.sge import sge_utils as sge
from jobmon.client.task import Task
from jobmon.models.task_instance import TaskInstance
from jobmon.models.task_instance_status import TaskInstanceStatus
from tests._scripts.timeout_and_skip import timeout_and_skip


path_to_file = os.path.dirname(__file__)


def test_valid_command(jlm_sge_daemon):
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



