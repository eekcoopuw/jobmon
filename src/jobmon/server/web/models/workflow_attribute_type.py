"""Workflow Attribute type database table."""
from jobmon.server.web.models import DB


class WorkflowAttributeType(DB.Model):
    """Workflow Attribute type database table."""

    __tablename__ = 'workflow_attribute_type'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        """Workflow attribute object from dict of params."""
        return cls(
            id=dct['workflow_attribute_type_id'],
            name=dct['name']
        )

    def to_wire(self):
        """Workflow attribute attributes to dict."""
        return {
            'workflow_attribute_type_id': self.id,
            'name': self.name
        }
