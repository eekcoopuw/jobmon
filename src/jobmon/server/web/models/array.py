"""Array Table for the Database."""

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.sql import func

from jobmon.serializers import SerializeDistributorArray
from jobmon.server.web.models import Base


class Array(Base):
    """Array Database object."""

    __tablename__ = "array"

    def to_wire_as_distributor_array(self) -> tuple:
        """Serialize executor task object."""
        serialized = SerializeDistributorArray.to_wire(
            array_id=self.id,
            max_concurrently_running=self.max_concurrently_running,
            name=self.name,
        )
        return serialized

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    task_template_version_id = Column(Integer)
    workflow_id = Column(Integer)
    max_concurrently_running = Column(Integer)
    created_date = Column(DateTime, default=func.now())
