import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from jobmon.models.sql_base import Base


logger = logging.getLogger(__name__)


class WorkflowRunStatus(Base):
    __tablename__ = 'workflow_run_status'

    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)
