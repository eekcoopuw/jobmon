import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from datetime import datetime

from jobmon.models.sql_base import Base

logger = logging.getLogger(__name__)


class JobInstanceStatusLog(Base):
    """The table in the database that logs the JobInstance statuses"""

    __tablename__ = 'job_instance_status_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    status = Column(
        String(1),
        ForeignKey('job_instance_status.id'),
        nullable=False)
    status_time = Column(DateTime, default=datetime.utcnow)
