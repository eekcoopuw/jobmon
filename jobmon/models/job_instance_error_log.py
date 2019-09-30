import logging
from datetime import datetime

from jobmon.models import DB

logger = logging.getLogger(__name__)


class JobInstanceErrorLog(DB.Model):
    """The table in the database that logs the error messages for
    job_instances
    """

    __tablename__ = 'job_instance_error_log'

    id = DB.Column(DB.Integer, primary_key=True)
    job_instance_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = DB.Column(DB.DateTime, default=datetime.utcnow)
    description = DB.Column(DB.Text(collation='utf8_general_ci'))

    job_instance = DB.relationship("JobInstance", back_populates="errors")
