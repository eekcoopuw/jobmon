import logging

from jobmon.models import DB

logger = logging.getLogger(__name__)


class JobInstanceStatus(DB.Model):
    """The table in the database that holds on the possible statuses for
    JobInstance
    """

    __tablename__ = 'job_instance_status'
    INSTANTIATED = 'I'
    NO_EXECUTOR_ID = 'W'
    LOST_TRACK = 'L'
    SUBMITTED_TO_BATCH_EXECUTOR = 'B'
    RUNNING = 'R'
    RESOURCE_ERROR = 'Z'
    UNKNOWN_ERROR = 'U'
    ERROR = 'E'
    DONE = 'D'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
