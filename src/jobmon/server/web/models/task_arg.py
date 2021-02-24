"""Task Arg table."""
from jobmon.server.web.models import DB


class TaskArg(DB.Model):
    """Task arg table."""

    __tablename__ = 'task_arg'

    task_id = DB.Column(DB.Integer, primary_key=True)
    arg_id = DB.Column(DB.Integer, primary_key=True)
    val = DB.Column(DB.String(255))
