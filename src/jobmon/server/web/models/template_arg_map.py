"""Template arg map table."""
from jobmon.server.web.models import DB


class TemplateArgMap(DB.Model):
    """Template Arg Map table."""

    __tablename__ = 'template_arg_map'

    task_template_version_id = DB.Column(
        DB.Integer, DB.ForeignKey('task_template_version.id'),
        primary_key=True)
    arg_id = DB.Column(
        DB.Integer, DB.ForeignKey('arg.id'), primary_key=True)
    arg_type_id = DB.Column(
        DB.Integer, DB.ForeignKey('arg_type.id'), primary_key=True)

    task_template_version = DB.relationship(
        "TaskTemplateVersion",
        back_populates="template_arg_map")
    argument = DB.relationship(
        "Arg",
        back_populates="template_arg_map")
    argument_type = DB.relationship(
        "ArgType",
        back_populates="template_arg_map")
