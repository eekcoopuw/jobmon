"""Task Template database table."""
from jobmon.serializers import SerializeClientTaskTemplate
from jobmon.server.web.models import DB


class TaskTemplate(DB.Model):
    """Task Template database table."""

    __tablename__ = "task_template"

    def to_wire_as_client_task_template(self) -> tuple:
        """Serialize Task Template."""
        # serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        # return serialized
        return SerializeClientTaskTemplate.to_wire(
            self.id, self.tool_version_id, self.name
        )

    id = DB.Column(DB.Integer, primary_key=True)
    tool_version_id = DB.Column(DB.Integer, DB.ForeignKey("tool_version.id"))
    name = DB.Column(DB.String(255))

    # orm relationship
    tool_versions = DB.relationship("ToolVersion", back_populates="task_templates")
    task_template_versions = DB.relationship(
        "TaskTemplateVersion", back_populates="task_template"
    )
