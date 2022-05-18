"""Node Table in the Database."""
from sqlalchemy import Column, Integer

from jobmon.server.web.models import Base


class Node(Base):
    """Node Table in the Database."""

    __tablename__ = "node"

    id = Column(Integer, primary_key=True)
    task_template_version_id = Column(Integer)
    node_args_hash = Column(Integer)
