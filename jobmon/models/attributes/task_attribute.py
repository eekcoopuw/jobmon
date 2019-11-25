from jobmon.models import DB

# NOTE: This import needs to be here to support the ForeignKey relationship,
# otherwise sqlalchemy gets confused about that table. There may be a
# different way to solve this that involves loading this module elsewhere,
# but this definitely works
from jobmon.models.attributes.task_instance_attribute_type import TaskInstanceAttributeType


class TaskAttribute(DB.Model):
    __tablename__ = 'task_attribute'

    id = DB.Column(DB.Integer, primary_key=True)
    task_id = DB.Column(DB.Integer, DB.ForeignKey('task.id'))
    attribute_type = DB.Column(DB.Integer,
                               DB.ForeignKey('task_attribute_type.id'))
    value = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['task_attribute_id'],
            task_instance_id=dct['task_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'task_attribute_id': self.id,
            'task_id': self.task_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }
