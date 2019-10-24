from jobmon.models import DB


class Edge(DB.Model):

    __tablename__ = 'edge'

    dag_id = DB.Column(DB.Integer, DB.ForeignKey('dag.id'))
    node_id = DB.Column(DB.Integer, DB.ForeignKey('node.id'))
    upstream_nodes = DB.Column(DB.Text)
    downstream_nodes = DB.Column(DB.Text)
