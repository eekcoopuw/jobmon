"""Node Table in the Database."""
from jobmon.server.web.models import DB


class Node(DB.Model):
    """Node Table in the Database."""

    __tablename__ = "node"

    id = DB.Column(DB.Integer, primary_key=True)
    task_template_version_id = DB.Column(DB.Integer)
    node_args_hash = DB.Column(DB.Integer)
