"""DAG Database Table."""
from jobmon.server.web.models import DB


class Dag(DB.Model):
    """DAG Database Table."""

    __tablename__ = 'dag'

    id = DB.Column(DB.Integer, primary_key=True)
    hash = DB.Column(DB.VARCHAR(150))
    created_date = DB.Column(DB.DateTime, default=None)

    workflow = DB.relationship("Workflow", back_populates="dag", lazy=True)
