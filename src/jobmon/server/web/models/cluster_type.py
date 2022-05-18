"""ClusterType table in the database."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from jobmon.serializers import SerializeClusterType
from jobmon.server.web.models import Base


class ClusterType(Base):
    """ClusterType table in the database."""

    __tablename__ = "cluster_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    package_location = Column(String(2500))

    # ORM relationships
    clusters = relationship("Cluster", back_populates="cluster_type")

    def to_wire_as_requested_by_client(self) -> tuple:
        """Serialize cluster object."""
        return SerializeClusterType.to_wire(self.id, self.name, self.package_location)
