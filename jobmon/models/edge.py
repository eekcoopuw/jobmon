from jobmon.models import DB


class Edge(DB.Model):

    __tablename__ = 'edge'

    dag_id = DB.Column(DB.Integer, DB.ForeignKey('dag.id'), primary_key=True)
    node_id = DB.Column(DB.Integer, DB.ForeignKey('node.id'), primary_key=True)
    upstream_nodes = DB.Column(DB.Text)
    downstream_nodes = DB.Column(DB.Text)
