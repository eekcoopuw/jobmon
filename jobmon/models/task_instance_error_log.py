import logging
from datetime import datetime

from jobmon.models import DB

logger = logging.getLogger(__name__)


class TaskInstanceErrorLog(DB.Model):
    """The table in the database that logs the error messages for
    task_instances
    """

    __tablename__ = 'task_instance_error_log'

    id = DB.Column(DB.Integer, primary_key=True)
    task_instance_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('task_instance.id'),
        nullable=False)
    error_time = DB.Column(DB.DateTime, default=datetime.utcnow)
    description = DB.Column(DB.Text(collation='utf8_general_ci'))

    task_instance = DB.relationship("TaskInstance", back_populates="errors")
