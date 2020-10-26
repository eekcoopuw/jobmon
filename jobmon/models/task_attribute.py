from jobmon.models import DB
from jobmon.models.task_attribute_type import TaskAttributeType


class TaskAttribute(DB.Model):
    __tablename__ = 'task_attribute'

    id = DB.Column(DB.Integer, primary_key=True)
    task_id = DB.Column(DB.Integer, DB.ForeignKey('task.id'))
    attribute_type_id = DB.Column(DB.Integer,
                                  DB.ForeignKey('task_attribute_type.id'))
    value = DB.Column(DB.String(2000))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['task_attribute_id'],
            task_id=dct['task_id'],
            attribute_type_id=dct['attribute_type_id'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'task_attribute_id': self.id,
            'task_id': self.task_id,
            'attribute_type_id': self.attribute_type_id,
            'value': self.value
        }
