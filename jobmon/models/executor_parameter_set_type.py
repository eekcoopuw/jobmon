import logging

from jobmon.models import DB

logger = logging.getLogger(__name__)


class ExecutorParameterSetType(DB.Model):
    """The table in the database that holds the possible statuses for the
    ExecutorParameterSet """

    __tablename__ = 'executor_parameter_set_type'
    ORIGINAL = 'O'
    VALIDATED = 'V'
    ADJUSTED = 'A'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
