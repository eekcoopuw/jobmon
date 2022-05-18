"""Task Template database table."""
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from jobmon.serializers import SerializeClientTaskTemplate
from jobmon.server.web.models import Base


class TaskTemplate(Base):
    """Task Template database table."""

    __tablename__ = "task_template"

    def to_wire_as_client_task_template(self) -> tuple:
        """Serialize Task Template."""
        # serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        # return serialized
        return SerializeClientTaskTemplate.to_wire(
            self.id, self.tool_version_id, self.name
        )

    id = Column(Integer, primary_key=True)
    tool_version_id = Column(Integer, ForeignKey("tool_version.id"))
    name = Column(String(255))

    # orm relationship
    tool_versions = relationship("ToolVersion", back_populates="task_templates")
    task_template_versions = relationship(
        "TaskTemplateVersion", back_populates="task_template"
    )
