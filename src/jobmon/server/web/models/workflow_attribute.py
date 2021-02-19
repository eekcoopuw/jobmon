"""Workflow Attribute Database Table."""
from jobmon.server.web.models import DB


class WorkflowAttribute(DB.Model):
    """Workflow Attribute Database Table."""

    __tablename__ = 'workflow_attribute'

    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'), primary_key=True)
    workflow_attribute_type_id = DB.Column(DB.Integer,
                                           DB.ForeignKey('workflow_attribute_type.id'),
                                           primary_key=True)
    value = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        """Create class object from dict."""
        return cls(
            workflow_id=dct['workflow_id'],
            workflow_attribute_type_id=dct['workflow_attribute_type_id'],
            value=dct['value']
        )

    def to_wire(self):
        """Send data."""
        return {
            'workflow_id': self.workflow_id,
            'workflow_attribute_type_id': self.workflow_attribute_type_id,
            'value': self.value
        }
