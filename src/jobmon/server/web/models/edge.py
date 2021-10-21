"""Edge Database table."""
from jobmon.server.web.models import DB


class Edge(DB.Model):
    """Database Table to record edges."""

    __tablename__ = "edge"

    dag_id = DB.Column(DB.Integer, primary_key=True)
    node_id = DB.Column(DB.Integer, primary_key=True)
    # Upstream and downstream nodes are edge lists (incoming and outgoing).
    # They are python lists serialized as strings
    # with syntax: "[node_id, node_id, node_id, ...]"
    # for implementation details refer to
    # jobmon/client/swarm/workflow/clientdag.py
    # method: _insert_edges()
    upstream_node_ids = DB.Column(DB.Text)
    downstream_node_ids = DB.Column(DB.Text)
