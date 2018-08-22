import logging
from sqlalchemy import Column, String

from jobmon.models.sql_base import Base


logger = logging.getLogger(__name__)


class JobStatus(Base):
    """The table in the database that holds on the possible statuses for Job"""
    __tablename__ = 'job_status'

    REGISTERED = 'G'
    QUEUED_FOR_INSTANTIATION = 'Q'
    INSTANTIATED = 'I'
    RUNNING = 'R'
    ERROR_RECOVERABLE = 'E'
    ERROR_FATAL = 'F'
    DONE = 'D'

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)
