"""ArgType table in the database."""
from jobmon.server.web.models import DB


class ArgType(DB.Model):
    """ArgType table in the database."""

    __tablename__ = 'arg_type'

    NODE_ARG = 1
    TASK_ARG = 2
    OP_ARG = 3

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    template_arg_map = DB.relationship("TemplateArgMap", back_populates="argument_type")
