from jobmon.models import DB


class CommandTemplateArgTypeMapping(DB.Model):

    __tablename__ = 'command_template_arg_type_mapping'

    task_template_version_id = DB.Column(
        DB.Integer, DB.ForeignKey('task_template_version.id'),
        primary_key=True)
    arg_id = DB.Column(
        DB.Integer, DB.ForeignKey('arg.id'), primary_key=True)
    arg_type_id = DB.Column(
        DB.Integer, DB.ForeignKey('arg_type.id'), primary_key=True)

    task_template_version = DB.relationship(
        "TaskTemplateVersion",
        back_populates="command_template_arg_type_mappings")
    argument = DB.relationship(
        "Arg",
        back_populates="command_template_arg_type_mappings")
    argument_type = DB.relationship(
        "ArgType",
        back_populates="command_template_arg_type_mappings")
