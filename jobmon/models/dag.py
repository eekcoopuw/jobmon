from sqlalchemy.sql import func
from jobmon.models import DB


class Dag(DB.Model):

    __tablename__ = 'dag'

    id = DB.Column(DB.Integer, primary_key=True)
    hash = DB.Column(DB.VARCHAR(150))
    created_date = DB.Column(DB.DateTime, default=func.now())

    workflow = DB.relationship("Workflow", back_populates="dag", lazy=True)
