"""ArgType table in the database."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from jobmon.server.web.models import Base


class ArgType(Base):
    """ArgType table in the database."""

    __tablename__ = "arg_type"

    NODE_ARG = 1
    TASK_ARG = 2
    OP_ARG = 3

    id = Column(Integer, primary_key=True)
    name = Column(String(255))

    template_arg_map = relationship("TemplateArgMap", back_populates="argument_type")
