import logging

from jobmon.models import DB


logger = logging.getLogger(__name__)


class JobStatus(DB.Model):
    """The table in the database that holds on the possible statuses for Job"""

    __tablename__ = 'job_status'

    REGISTERED = 'G'
    QUEUED_FOR_INSTANTIATION = 'Q'
    INSTANTIATED = 'I'
    RUNNING = 'R'
    ERROR_RECOVERABLE = 'E'
    ERROR_FATAL = 'F'
    DONE = 'D'

    @classmethod
    def from_wire(cls, dct):
        return cls(id=dct['id'],
                   label=dct['label'],
                   viz_label=dct['viz_label'])

    def to_wire(self):
        return {
            'id': self.id,
            'label': self.label,
            'viz_label': self.viz_label
        }

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
    viz_label = DB.Column(DB.String(150), nullable=False)


# not sure where to put this mapping
_viz_label_mapping = {
    "REGISTERED": "PENDING",
    "QUEUED_FOR_INSTANTIATION": "PENDING",
    "INSTANTIATED": "PENDING",
    "RUNNING": "RUNNING",
    "ERROR_RECOVERABLE": "RECOVERING",
    "ERROR_FATAL": "FATAL",
    "DONE": "DONE",
}
