from jobmon.server.web.models import DB

from sqlalchemy.sql import func


class TaskInstanceErrorLog(DB.Model):
    """The table in the database that logs the error messages for
    task_instances
    """

    __tablename__ = 'task_instance_error_log'

    id = DB.Column(DB.Integer, primary_key=True)
    task_instance_id = DB.Column(DB.Integer, DB.ForeignKey('task_instance.id'))
    error_time = DB.Column(DB.DateTime, default=func.now())
    description = DB.Column(DB.Text(collation='utf8_general_ci'))

    task_instance = DB.relationship("TaskInstance", back_populates="errors")
