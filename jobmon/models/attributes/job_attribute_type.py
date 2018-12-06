from jobmon.models import DB


class JobAttributeType(DB.Model):
    __tablename__ = 'job_attribute_type'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))
    type = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['job_attribute_type_id'],
            name=dct['name'],
            type=dct['type']
        )

    def to_wire(self):
        return {
            'job_attribute_type_id': self.id,
            'name': self.name,
            'type': self.type
        }
