"""Node arg db table."""
from jobmon.server.web.models import DB


class NodeArg(DB.Model):
    """Node arg db table."""

    __tablename__ = 'node_arg'

    node_id = DB.Column(DB.Integer, primary_key=True)
    arg_id = DB.Column(DB.Integer, primary_key=True)
    val = DB.Column(DB.String(255))
