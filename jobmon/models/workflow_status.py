from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from jobmon.models.sql_base import Base


class WorkflowStatus(Base):
    __tablename__ = 'workflow_status'

    CREATED = 'C'
    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = Column(
        String(1), primary_key=True)
    label = Column(
        String(150), nullable=False)
