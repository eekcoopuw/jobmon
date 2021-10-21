"""Task Attribute Type table."""
from typing import Any, Dict

from jobmon.server.web.models import DB


class TaskAttributeType(DB.Model):
    """Task Attribute Type Table."""

    __tablename__ = "task_attribute_type"

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls: Any, dct: Dict) -> Any:
        """Task Attribute Type object parsed from dict."""
        return cls(id=dct["task_attribute_type_id"], name=dct["name"])

    def to_wire(self) -> Dict:
        """Returns dict of TaskAttributeType attributes."""
        return {"task_attribute_type_id": self.id, "name": self.name}
