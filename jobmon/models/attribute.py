from jobmon.models import DB


class Attribute(DB.Model):

    __tablename__ = 'attribute'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.VARCHAR(255))
