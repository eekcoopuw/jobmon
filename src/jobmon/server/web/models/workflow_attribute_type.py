"""Workflow Attribute type database table."""
from typing import Any, Dict

from jobmon.server.web.models import DB


class WorkflowAttributeType(DB.Model):
    """Workflow Attribute type database table."""

    __tablename__ = "workflow_attribute_type"

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls: Any, dct: Dict) -> Any:
        """Workflow attribute object from dict of params."""
        return cls(id=dct["workflow_attribute_type_id"], name=dct["name"])

    def to_wire(self) -> Dict:
        """Workflow attribute attributes to dict."""
        return {"workflow_attribute_type_id": self.id, "name": self.name}
