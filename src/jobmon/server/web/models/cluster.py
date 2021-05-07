"""Cluster Table in the Database."""
from jobmon.serializers import SerializeCluster
from jobmon.server.web.models import DB


class Cluster(DB.Model):
    """Cluster Table in the Database."""

    __tablename__ = 'cluster'

    def to_wire_as_requested_by_client(self):
        """Serialize cluster object."""
        return SerializeCluster.to_wire(self.id,
                                        self.name,
                                        self.cluster_type.name,
                                        self.connection_string)

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))
    cluster_type_id = DB.Column(DB.Integer, DB.ForeignKey('cluster_type.id'))
    connection_string = DB.Column(DB.String(2500))

    # ORM relationships
    cluster_type = DB.relationship("ClusterType", back_populates="clusters")
    queues = DB.relationship("Queue", back_populates="cluster")
