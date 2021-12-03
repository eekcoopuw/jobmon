import pytest
from jobmon.client.task_resources import TaskResources


def test_task_resources_hash():

    class MockConcreteResource:
        """Mock the concrete resource object with just a resources object."""
        def __init__(self, resource_dict: dict):
            self.resources = resource_dict

    # keys purposefully out of order
    resources_dict = {
        'd': 'string datatype',
        'b': 123,
        'a': ['list', 'data', 'type']
    }

    # same values, different representation order. Should not matter
    resource_dict_sorted = {key: resources_dict[key] for key in sorted(resources_dict)}

    # resources 1 and 2 should be equal, 3 should be different
    resource_1 = MockConcreteResource(resources_dict)
    resource_2 = MockConcreteResource(resource_dict_sorted)
    resource_3 = MockConcreteResource(dict(resources_dict, b=100))

    tr1 = TaskResources(
        task_resources_type_id='no matter',
        concrete_resources=resource_1)
    tr2 = TaskResources(
        task_resources_type_id='no matter',
        concrete_resources=resource_2)
    tr1_clone = TaskResources('no matter', resource_1)
    tr3 = TaskResources('no matter', resource_3)

    assert tr1 == tr1_clone
    assert tr1 == tr2
    assert tr1 != tr3

    assert len({tr1, tr2, tr3}) == 2

