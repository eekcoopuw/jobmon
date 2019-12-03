from jobmon.models import DB


class TaskAttributeType(DB.Model):
    __tablename__ = 'task_attribute_type'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['task_attribute_type_id'],
            name=dct['name']
        )

    def to_wire(self):
        return {
            'task_attribute_type_id': self.id,
            'name': self.name
        }
