from jobmon.serializers import SerializeClientToolVersion
from jobmon.server.web.models import DB


class ToolVersion(DB.Model):

    __tablename__ = 'tool_version'

    def to_wire_as_client_tool_version(self) -> tuple:
        serialized = SerializeClientToolVersion.to_wire(
            id=self.id,
            tool_id=self.tool_id)
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    tool_id = DB.Column(DB.Integer, DB.ForeignKey('tool.id'),
                        nullable=False)

    # ORM relationships
    tool = DB.relationship("Tool", back_populates="tool_versions")
    task_templates = DB.relationship(
        "TaskTemplate", back_populates="tool_versions")
