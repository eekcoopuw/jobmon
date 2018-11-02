import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from datetime import datetime

from jobmon.models.sql_base import Base
from jobmon.models.job_instance import JobInstance

logger = logging.getLogger(__name__)


class JobInstanceErrorLog(Base):
    """The table in the database that logs the error messages for
    job_instances
    """

    __tablename__ = 'job_instance_error_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = Column(DateTime, default=datetime.utcnow)
    description = Column(Text)

    job_instance = relationship("JobInstance", back_populates="errors")
