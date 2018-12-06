from jobmon.models import DB


class JobAttribute(DB.Model):
    __tablename__ = 'job_attribute'

    id = DB.Column(DB.Integer, primary_key=True)
    job_id = DB.Column(DB.Integer, DB.ForeignKey('job.job_id'))
    attribute_type = DB.Column(DB.Integer,
                               DB.ForeignKey('job_attribute_type.id'))
    value = DB.Column(DB.String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['job_attribute_id'],
            job_id=dct['job_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'job_attribute_id': self.id,
            'job_id': self.job_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }
