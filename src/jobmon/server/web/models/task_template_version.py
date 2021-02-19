"""Database Table for Task Template Versions."""
from jobmon.serializers import SerializeClientTaskTemplateVersion
from jobmon.server.web.models import DB


class TaskTemplateVersion(DB.Model):
    """Database Table for Task Template Versions."""

    __tablename__ = 'task_template_version'

    def to_wire_as_client_task_template_version(self) -> tuple:
        """Serialized Task Template Version objects."""
        # serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        # return serialized
        id_name_map = {}
        for arg_mapping in self.template_arg_map:
            id_name_map[arg_mapping.argument.name] = arg_mapping.argument.id
        return SerializeClientTaskTemplateVersion.to_wire(self.id, id_name_map)

    id = DB.Column(DB.Integer, primary_key=True)
    task_template_id = DB.Column(DB.Integer, DB.ForeignKey('task_template.id'))
    command_template = DB.Column(DB.Text(collation='utf8_general_ci'))
    arg_mapping_hash = DB.Column(DB.Integer)

    # orm relationship
    task_template = DB.relationship("TaskTemplate", back_populates="task_template_versions")
    template_arg_map = DB.relationship(
        "TemplateArgMap",
        back_populates="task_template_version"
    )
