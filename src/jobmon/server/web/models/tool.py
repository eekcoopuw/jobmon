"""Tool DB Table."""
from jobmon.serializers import SerializeClientTool
from jobmon.server.web.models import DB


class Tool(DB.Model):
    """Tool DB Table."""

    __tablename__ = "tool"

    def to_wire_as_client_tool(self) -> tuple:
        """Serialize tool object."""
        serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255), nullable=False)

    tool_versions = DB.relationship("ToolVersion", back_populates="tool")
