from jobmon.client.client_logging import ClientLogging as logging
from jobmon.serializers import SerializeSwarmTask

logger = logging.getLogger(__name__)


class SwarmTask:
    """
    This is a simplified Task object used on the RESTful API client side
    when only task_id, task_args_hash, and status are needed.
    """

    def __init__(self, task_id: int, status: str, task_args_hash: int):
        # Takes one row of the SQL query return
        self.task_id = task_id
        logger.debug("task_id" + str(task_id))
        self.status = status
        logger.debug("status: " + str(status))
        self.task_args_hash = task_args_hash

    @classmethod
    def from_wire(cls, wire_tuple: tuple):
        kwargs = SerializeSwarmTask.kwargs_from_wire(wire_tuple)
        return cls(task_id=kwargs["task_id"], status=kwargs["status"],
                   task_args_hash=kwargs["task_args_hash"])
