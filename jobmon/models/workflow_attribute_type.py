from jobmon.models import DB


class WorkflowAttributeType(DB.Model):
    __tablename__ = 'workflow_attribute_type'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_attribute_type_id'],
            name=dct['name']
        )

    def to_wire(self):
        return {
            'workflow_attribute_type_id': self.id,
            'name': self.name
        }
