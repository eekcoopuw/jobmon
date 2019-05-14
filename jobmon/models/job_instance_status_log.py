import logging
from datetime import datetime

from jobmon.models import DB

logger = logging.getLogger(__name__)


class JobInstanceStatusLog(DB.Model):
    """The table in the database that logs the JobInstance statuses"""

    __tablename__ = 'job_instance_status_log'

    id = DB.Column(DB.Integer, primary_key=True)
    job_instance_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    status = DB.Column(
        DB.String(1),
        DB.ForeignKey('job_instance_status.id'),
        nullable=False)
    status_time = DB.Column(DB.DateTime, default=datetime.utcnow)
