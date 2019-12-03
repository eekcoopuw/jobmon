from jobmon.models import DB


class TaskInstanceAttributeType(DB.Model):
    __tablename__ = 'task_instance_attribute_type'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))
    type = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['task_instance_attribute_type_id'],
            name=dct['name'],
            type=dct['type']
        )

    def to_wire(self):
        return {
            'task_instance_attribute_type_id': self.id,
            'name': self.name,
            'type': self.type
        }
