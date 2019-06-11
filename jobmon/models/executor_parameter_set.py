import logging

from jobmon.models import DB
from jobmon.models.executor_parameter_set_type import ExecutorParameterSetType


logger = logging.getLogger(__name__)


class ExecutorParameterSet(DB.Model):
    """The table in the database that holds all executor specific parameters"""

    __tablename__ = 'executor_parameter_set'

    id = DB.Column(DB.Integer, primary_key=True)
    job_id = DB.Column(DB.Integer, DB.ForeignKey('job.job_id'), nullable=False)
    parameter_set_type = DB.Column(
        DB.String(1), DB.ForeignKey('executor_parameter_set_type.id'),
        nullable=False)

    # enforce runtime limit if executor implements terminate_timed_out_jobs
    max_runtime_seconds = DB.Column(DB.Integer, default=None)

    # free text field of arguments passed unadultered to executor
    context_args = DB.Column(DB.String(1000), default=None)

    # sge specific parameters
    queue = DB.Column(DB.String(255), default=None)
    num_cores = DB.Column(DB.Integer, default=None)
    m_mem_free = DB.Column(DB.Float, default=None)
    j_resource = DB.Column(DB.Boolean, default=None)

    # ORM relationships
    job = DB.relationship("Job", foreign_keys=[job_id])

    def activate(self):
        self.job.executor_parameter_set_id = self.id
