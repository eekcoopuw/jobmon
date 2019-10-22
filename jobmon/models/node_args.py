from jobmon.models import DB


class NodeArg(DB.Model):

    __tablename__ = 'node_args'

    node_id = DB.Column(DB.Integer, DB.ForeignKey('node.id'), primary_key=True)
    arg_id = DB.Column(DB.Integer, DB.ForeignKey('args.id'), primary_key=True)
    val = DB.Column(DB.String(255))
