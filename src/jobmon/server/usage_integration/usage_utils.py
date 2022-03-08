from functools import total_ordering
from typing import Any


@total_ordering
class QueuedTI:
    """This is a class to hold the task instance.

    So far we only need actual distributor_id and cluster id.
    TODO: in 3.1, the actual distributor_id will be subtask_id in DB.
    """

    def __init__(
        self,
        task_instance_id: int,
        distributor_id: int,
        cluster_type_name: str,
        cluster_id: int,
    ) -> None:
        """Constructor."""
        self.task_instance_id = task_instance_id
        self.distributor_id = distributor_id
        self.cluster_type_name = cluster_type_name
        self.cluster_id = cluster_id
        self.age = 0

    def __str__(self) -> str:
        """Turn to string."""
        return (
            f"task_instance_id: {self.task_instance_id}, "
            f"age: {self.age}, "
            f"distributor_id: {self.distributor_id}, "
            f"cluster_name: {self.cluster_type_name}, "
            f"cluster_id: {self.cluster_id}, "
        )

    def __eq__(self, other: Any) -> bool:
        """Provide a method to compare."""
        return self.age == other.age

    def __lt__(self, other: Any) -> bool:
        """Provide a method to compare."""
        return self.age < other.age

    def __hash__(self) -> int:
        """Use task instance id as a hash."""
        return self.task_instance_id
