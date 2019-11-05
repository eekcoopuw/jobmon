from jobmon.models import DB


class NodeArg(DB.Model):

    __tablename__ = 'node_arg'

    node_id = DB.Column(DB.Integer, primary_key=True)
    arg_id = DB.Column(DB.Integer, primary_key=True)
    val = DB.Column(DB.String(255))
