from jobmon.models import DB

# NOTE: This import needs to be here to support the ForeignKey relationship,
# otherwise sqlalchemy gets confused about that table. There may be a
# different way to solve this that involves loading this module elsewhere,
# but this definitely works
from jobmon.models.attributes.workflow_run_attribute_type import \
    WorkflowRunAttributeType


class WorkflowRunAttribute(DB.Model):
    __tablename__ = 'workflow_run_attribute'

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_run_id = DB.Column(DB.Integer, DB.ForeignKey('workflow_run.id'))
    attribute_type = DB.Column(DB.Integer,
                               DB.ForeignKey('workflow_run_attribute_type.id'))
    value = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_run_attribute_id'],
            workflow_run_id=dct['workflow_run_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'workflow_run_attribute_id': self.id,
            'workflow_run_id': self.workflow_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }
