from jobmon.serializers import SerializeClientTool
from jobmon.server.web.models import DB


class Tool(DB.Model):

    __tablename__ = 'tool'

    def to_wire_as_client_tool(self) -> tuple:
        serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255), nullable=False)

    tool_versions = DB.relationship("ToolVersion", back_populates="tool")
