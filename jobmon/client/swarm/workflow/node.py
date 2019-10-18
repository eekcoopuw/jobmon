import hashlib


class Node(object):
    """

    """

    def __init__(self, dag_id: int, task_template_version_id: int,
                 node_arg_hash: int):
        """
        Args:
            dag_id:
            task_template_version_id:
            node_arg_hash:
        """
        self.dag_id = dag_id
        self.task_template_version_id = task_template_version_id
        self.node_arg_hash = node_arg_hash

    def bind(self) -> int:
        """Retrieve an id for a matching node in the database. If it doesn't
        exist, first create one.

        Returns
        """
        return 1
