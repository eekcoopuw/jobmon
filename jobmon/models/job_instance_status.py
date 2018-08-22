import logging

from sqlalchemy import Column, String

from jobmon.models.sql_base import Base

logger = logging.getLogger(__name__)


class JobInstanceStatus(Base):
   """The table in the database that holds on the possible statuses for JobInstance"""__tablename__ = 'job_instance_status'

    INSTANTIATED = 'I'
    SUBMITTED_TO_BATCH_EXECUTOR = 'B'
    RUNNING = 'R'
    ERROR = 'E'
    DONE = 'D'

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)
