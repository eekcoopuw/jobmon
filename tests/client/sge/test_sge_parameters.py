import os
from functools import partial
from time import sleep
import pytest

from jobmon.client.execution.strategies.sge import sge_utils as sge
from jobmon.client.execution.strategies.sge.sge_parameters import SGEParameters
from jobmon.client.templates.bash_task import BashTask
from jobmon.client.templates.python_task import PythonTask
from jobmon.models.task import Task


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


@pytest.mark.unittest
@pytest.mark.parametrize('mem', ['1TB', '752GB'])
def test_big_memory_adjusted(mem):
    task = BashTask(command=f"{os.path.join(thisdir, 'jmtest.sh')}",
                  executor_class="SGEExecutor",
                  name="Invalid memory",
                  num_cores=1,
                  max_runtime_seconds=600,
                  m_mem_free=mem,
                  max_attempts=1,
                  j_resource=True,
                  queue="all.q")
    resources = task.executor_parameters()
    msg = resources.is_valid()[1] # get the ExecutorParameter object
    assert "\n Memory" in msg
    resources.validate()
    assert 750 == resources.m_mem_free







