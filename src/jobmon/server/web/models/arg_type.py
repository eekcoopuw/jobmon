"""ArgType table in the database."""
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship, Session

from jobmon.server.web.models import Base


class ArgType(Base):
    """ArgType table in the database."""

    __tablename__ = "arg_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(255))

    template_arg_map = relationship("TemplateArgMap", back_populates="argument_type")


def add_arg_types(session: Session):
    types = [
        ArgType(id=1, name="NODE_ARG"),
        ArgType(id=2, name="TASK_ARG"),
        ArgType(id=3, name="OP_ARG")
    ]
    session.add_all(types)
