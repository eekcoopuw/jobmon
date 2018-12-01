from datetime import datetime

from jobmon.models import DB


class TaskDagMeta(DB.model):
    """ A DAG of Tasks."""

    __tablename__ = 'task_dag'
    """The table in the database that holds all info on TaskDags"""

    dag_id = DB.Column(DB.Integer, primary_key=True)
    dag_hash = DB.Column(DB.String(150))
    name = DB.Column(DB.String(150))
    user = DB.Column(DB.String(150))
    heartbeat_date = DB.Column(DB.DateTime, default=datetime.utcnow)
    created_date = DB.Column(DB.DateTime, default=datetime.utcnow)

    workflow = DB.relationship("Workflow", back_populates="task_dag")
