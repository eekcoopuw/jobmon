import pytest
from jobmon.client.task_resources import TaskResources
from jobmon.cluster_type.base import ConcreteResource


def test_task_resources_hash():

    class MockConcreteResource(ConcreteResource):
        """Mock the concrete resource object."""
        def __init__(self, resource_dict: dict):
            self._resources = resource_dict

        @property
        def queue(self):
            class MockQueue:
                queue_name: str = "mock-queue"
            return MockQueue()

        @property
        def resources(self):
            return self._resources

        @classmethod
        def validate_and_create_concrete_resource(cls, *args, **kwargs):
            pass

        @classmethod
        def adjust_and_create_concrete_resource(cls, *args, **kwargs):
            pass

    # keys purposefully out of order
    resources_dict = {
        'd': 'string datatype',
        'b': 123,
        'a': ['list', 'data', 'type']
    }

    # same values, different representation order. Should not affect object equality.
    resource_dict_sorted = {key: resources_dict[key] for key in sorted(resources_dict)}

    # resources 1 and 2 should be equal, 3 should be different
    resource_1 = MockConcreteResource(resources_dict)
    resource_2 = MockConcreteResource(resource_dict_sorted)
    resource_3 = MockConcreteResource(dict(resources_dict, b=100))

    tr1 = TaskResources(
        task_resources_type_id='O',
        concrete_resources=resource_1)
    tr2 = TaskResources(
        task_resources_type_id='O',
        concrete_resources=resource_2)
    tr1_clone = TaskResources('O', resource_1)
    tr3 = TaskResources('O', resource_3)

    assert tr1 == tr1_clone
    assert tr1 == tr2
    assert tr1 != tr3

    assert len({tr1, tr2, tr3}) == 2

    # Equality instance check - should be false if other is not a ConcreteResource object
    class FakeResource:
        def __hash__(self):
            return hash(resource_1)

    assert not resource_1 == FakeResource()
