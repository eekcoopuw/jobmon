from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import relationship

from jobmon.models.sql_base import Base


class TaskDagMeta(Base):
    """
    A DAG of Tasks.
    """
    __tablename__ = 'task_dag'
    """The table in the database that holds all info on TaskDags"""

    dag_id = Column(Integer, primary_key=True)
    dag_hash = Column(String(150))
    name = Column(String(150))
    user = Column(String(150))
    heartbeat_date = Column(DateTime, default=datetime.utcnow)
    created_date = Column(DateTime, default=datetime.utcnow)

    workflow = relationship("WorkflowDAO", back_populates="task_dag")
