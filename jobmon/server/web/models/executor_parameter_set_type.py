from jobmon.constants import ExecutorParameterSetType as Types
from jobmon.server.web.models import DB


class ExecutorParameterSetType(DB.Model):
    """The table in the database that holds the possible statuses for the
    ExecutorParameterSet """

    __tablename__ = 'executor_parameter_set_type'
    ORIGINAL = Types.ORIGINAL
    VALIDATED = Types.VALIDATED
    ADJUSTED = Types.ADJUSTED

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
