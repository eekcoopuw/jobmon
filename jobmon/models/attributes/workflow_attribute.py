from jobmon.models import DB


class WorkflowAttribute(DB.Model):
    __tablename__ = 'workflow_attribute'

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    attribute_type = DB.Column(DB.Integer,
                               DB.ForeignKey('workflow_attribute_type.id'))
    value = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_attribute_id'],
            workflow_id=dct['workflow_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'workflow_attribute_id': self.id,
            'workflow_id': self.workflow_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }
