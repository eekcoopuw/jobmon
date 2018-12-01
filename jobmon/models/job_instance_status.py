import logging

from jobmon.models import DB

logger = logging.getLogger(__name__)


class JobInstanceStatus(DB.model):
    """The table in the database that holds on the possible statuses for
    JobInstance
    """

    __tablename__ = 'job_instance_status'
    INSTANTIATED = 'I'
    SUBMITTED_TO_BATCH_EXECUTOR = 'B'
    RUNNING = 'R'
    ERROR = 'E'
    DONE = 'D'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
