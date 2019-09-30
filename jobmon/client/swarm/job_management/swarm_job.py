from jobmon.client.client_logging import ClientLogging as logging
from jobmon.serializers import SerializeSwarmJob

logger = logging.getLogger(__name__)


class SwarmJob:
    """
    This is a simplified Job object used on the RESTful API client side
    when only job_id, job_hash, and status are needed.
    """

    def __init__(self, job_id: int, status: str, job_hash: int):
        # Takes one row of the SQL query return
        self.job_id = job_id
        logger.debug("job_id" + str(job_id))
        self.status = status
        logger.debug("status: " + str(status))
        self.job_hash = job_hash

    @classmethod
    def from_wire(cls, wire_tuple: tuple):
        kwargs = SerializeSwarmJob.kwargs_from_wire(wire_tuple)
        return cls(job_id=kwargs["job_id"], status=kwargs["status"],
                   job_hash=kwargs["job_hash"])
