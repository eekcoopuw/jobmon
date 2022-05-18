"""Queue Table in the Database."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from jobmon.serializers import SerializeQueue
from jobmon.server.web.models import Base


class Queue(Base):
    """Queue Table in the Database."""

    __tablename__ = "queue"

    def to_wire_as_requested_by_client(self) -> tuple:
        """Serialize cluster object."""
        return SerializeQueue.to_wire(self.id, self.name, self.parameters)

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    cluster_id = Column(Integer, ForeignKey("cluster.id"))
    parameters = Column(String(2500))

    # ORM relationships
    cluster = relationship("Cluster", back_populates="queues")
