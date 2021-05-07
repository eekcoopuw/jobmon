"""Queue Table in the Database."""
from jobmon.serializers import SerializeQueue
from jobmon.server.web.models import DB


class Queue(DB.Model):
    """Queue Table in the Database."""

    __tablename__ = 'queue'

    def to_wire_as_requested_by_client(self):
        """Serialize cluster object."""
        return SerializeQueue.to_wire(self.id,
                                      self.name,
                                      self.cluster.name,
                                      self.cluster.cluster_type.name,
                                      self.parameters)

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))
    cluster_id = DB.Column(DB.Integer, DB.ForeignKey('cluster.id'))
    parameters = DB.Column(DB.String(2500))

    # ORM relationships
    cluster = DB.relationship("Cluster", back_populates="queues")
