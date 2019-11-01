from jobmon.models import DB


class Dag(DB.Model):

    __tablename__ = 'dag'

    id = DB.Column(DB.Integer, primary_key=True)
    hash = DB.Column(DB.VARCHAR(255))
