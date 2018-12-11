from jobmon.models import DB

# NOTE: This import needs to be here to support the ForeignKey relationship,
# otherwise sqlalchemy gets confused about that table. There may be a
# different way to solve this that involves loading this module elsewhere,
# but this definitely works
from jobmon.models.attributes.job_attribute_type import JobAttributeType


class JobAttribute(DB.Model):

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
