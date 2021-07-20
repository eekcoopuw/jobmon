"""ClusterType table in the database."""
from jobmon.serializers import SerializeClusterType
from jobmon.server.web.models import DB


class ClusterType(DB.Model):
    """ClusterType table in the database."""

    __tablename__ = 'cluster_type'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))
    package_location = DB.Column(DB.String(2500))

    # ORM relationships
    clusters = DB.relationship("Cluster", back_populates="cluster_type")

    def to_wire_as_requested_by_client(self) -> tuple:
        """Serialize cluster object."""
        return SerializeClusterType.to_wire(self.id, self.name, self.package_location)
