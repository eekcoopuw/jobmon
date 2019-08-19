import logging

from jobmon.models import DB


logger = logging.getLogger(__name__)


class JobStatus(DB.Model):
    """The table in the database that holds on the possible statuses for Job"""

    __tablename__ = 'job_status'

    QUEUED_FOR_INSTANTIATION = 'Q'
    INSTANTIATED = 'I'
    RUNNING = 'R'
    ERROR_RECOVERABLE = 'E'
    ADJUSTING_RESOURCES = 'A'
    ERROR_FATAL = 'F'
    DONE = 'D'

    @classmethod
    def from_wire(cls, dct):
        return cls(id=dct['id'],
                   label=dct['label'])

    def to_wire(self):
        return {
            'id': self.id,
            'label': self.label
        }

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
