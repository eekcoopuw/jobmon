from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from jobmon.sql_base import Base


class TaskDagMeta(Base):
    """
    A DAG of Tasks.
    """
    __tablename__ = 'task_dag'

    dag_id = Column(Integer, primary_key=True)
    dag_hash = Column(String(150))
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime, default=datetime.utcnow())
