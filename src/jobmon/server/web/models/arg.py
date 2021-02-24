"""Arg table in the database."""
from jobmon.server.web.models import DB


class Arg(DB.Model):
    """Arg table in the database."""

    __tablename__ = 'arg'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    template_arg_map = DB.relationship("TemplateArgMap", back_populates="argument")
