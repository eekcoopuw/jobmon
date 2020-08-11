import os
from functools import partial
from time import sleep
import pytest

from jobmon.client.execution.strategies.sge import sge_utils as sge
from jobmon.client.execution.strategies.sge.sge_parameters import SGEParameters
from jobmon.client.templates.python_task import PythonTask
from jobmon.models.task import Task


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


@pytest.mark.unittest
@pytest.mark.parametrize("mem_input,expected", [('.5TB', 500), ('500GB', 500),
                                                ('500000MB', 500), ('.129GB', 0.129),
                                                ('129MB', 0.129), ('0B', 1),
                                                ('10gigabytes', 1)])
def test_memory_transformed_correctly(mem_input, expected):
    resource = SGEParameters(m_mem_free=mem_input, num_cores=1, queue='all.q',
                             max_runtime_seconds=86400)
    resource.validate()
    assert resource.m_mem_free == expected


@pytest.mark.unittest
@pytest.mark.parametrize("mem_input,expected,q", [('1TB', 750, 'all.q'), ('751G', 750, 'long.q'),
                                                  ('120MB', 0.128, 'all.q'), ('0B', 1, 'i.q'),
                                                  ('751G', 751, 'geospatial.q')])
def test_memory_resource_validate(mem_input, expected, q):
    resource = SGEParameters(m_mem_free=mem_input, num_cores=1, queue=q,
                             max_runtime_seconds=120)
    resource.validate()
    assert resource.m_mem_free == expected


@pytest.mark.unittest
def test_number_cores():
    resource = SGEParameters(m_mem_free="1G", num_cores=1, queue='all.q',
                             max_runtime_seconds=86400)
    resource.validate()
    assert resource.num_cores == 1


@pytest.mark.unittest
def test_no_core():
    resource = SGEParameters(m_mem_free="1G", queue='all.q',
                             max_runtime_seconds=86400)
    resource.validate()
    assert resource.num_cores == 1


@pytest.mark.unittest
@pytest.mark.parametrize("input, expect", [(0, 24 * 60 * 60), (-1, 24 * 60 * 60), (120, 120)])
def test_max_runtime_seconds(input, expect):
    resource = SGEParameters(m_mem_free="1G", queue='all.q',
                             max_runtime_seconds=input)
    resource.validate()
    assert resource.max_runtime_seconds == expect


@pytest.mark.unittest
def test_hard_limit_true():
    resource = SGEParameters(m_mem_free="1G", queue='all.q', hard_limits=True,
                             max_runtime_seconds=1382402)
    resource.validate()
    assert resource.queue == 'all.q'
    assert resource.max_runtime_seconds == 259200


@pytest.mark.unittest
def test_hard_limit_false():
    resource = SGEParameters(m_mem_free="1G", queue='all.q', hard_limits=False,
                             max_runtime_seconds=1382402)
    resource.validate()
    assert resource.queue == 'long.q'
    assert resource.max_runtime_seconds == 1382400


@pytest.mark.unittest
@pytest.mark.parametrize("input, expect", [({'max_runtime_seconds': 0.5}, 0.5), ({'max_runtime_seconds': 0}, 0),
                                           ({'max_runtime_seconds': 1}, 1), ({'max_runtime_seconds': -1}, 0.5),
                                           ({'max_runtime_seconds': 0.3}, 0.3), ({'max_runtime_seconds': 1.2}, 0.5)])
def test_resource_scale(input, expect):
    resource = SGEParameters(m_mem_free="1G", queue='all.q', j_resource= True,
                             max_runtime_seconds=120, resource_scales=input)
    resource.validate()
    assert resource.resource_scales["max_runtime_seconds"] == expect


@pytest.mark.unittest
def test_allq_to_longq():
    resources = SGEParameters(m_mem_free="1G", queue="all.q", max_runtime_seconds=259300)
    msg = resources.validation_msg()
    assert "\n Queue" in msg
    resources.validate()
    assert resources.queue == "long.q"


@pytest.mark.unittest
def test_all_to_long_max():
    from jobmon.client.execution.strategies.sge.sge_queue import SGE_LONG_Q
    resources = SGEParameters(m_mem_free="1G", queue="all.q", max_runtime_seconds=1036800,
                              j_resource=True, resource_scales={'max_runtime_seconds': 0.5})
    resources.validate()
    resources._scale_max_runtime_seconds()
    assert resources.queue == "long.q"
    assert resources.max_runtime_seconds == SGE_LONG_Q.max_runtime_seconds