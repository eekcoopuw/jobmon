from jobmon.models import DB


class TaskTemplate(DB.Model):

    __tablename__ = 'task_template'

    def to_wire_as_client_task_template(self) -> tuple:
        # serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        # return serialized
        pass

    id = DB.Column(DB.Integer, primary_key=True)
    tool_version_id = DB.Column(DB.Integer, DB.ForeignKey('tool_version.id'),
                                nullable=False)
    name = DB.Column(DB.String(255), nullable=False)

    # orm relationship
    tool_versions = DB.relationship(
        "ToolVersion", back_populates="task_templates")
    task_template_versions = DB.relationship(
        "TaskTemplateVersion", back_populates="task_template")
