"""Cluster Table in the Database."""
from typing import Tuple

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from jobmon.serializers import SerializeCluster
from jobmon.server.web.models import Base


class Cluster(Base):
    """Cluster Table in the Database."""

    __tablename__ = "cluster"

    def to_wire_as_requested_by_client(self) -> Tuple:
        """Serialize cluster object."""
        return SerializeCluster.to_wire(
            self.id,
            self.name,
            self.cluster_type.name,
            self.cluster_type.package_location,
            self.connection_parameters,
        )

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    cluster_type_id = Column(Integer, ForeignKey("cluster_type.id"))
    connection_parameters = Column(String(2500))

    # ORM relationships
    cluster_type = relationship("ClusterType", back_populates="clusters")
    queues = relationship("Queue", back_populates="cluster")
