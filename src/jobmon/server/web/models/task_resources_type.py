"""Task Resources Type Database Table."""
from jobmon.constants import TaskResourcesType as Types
from jobmon.server.web.models import DB


class TaskResourcesType(DB.Model):
    """The table in the database that holds the possible statuses for the TaskResources."""

    __tablename__ = "task_resources_type"
    ORIGINAL = Types.ORIGINAL
    VALIDATED = Types.VALIDATED
    ADJUSTED = Types.ADJUSTED

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
