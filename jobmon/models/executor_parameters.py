import logging

from jobmon.models import DB


logger = logging.getLogger(__name__)


class ExecutorParameters(DB.Model):
    """The table in the database that holds all executor specific parameters"""

    __tablename__ = 'executor_parameters'

    id = DB.Column(DB.Integer, primary_key=True)
    job_id = DB.Column(DB.Integer, DB.ForeignKey('job.job_id'), nullable=False)

    # enforce runtime limit if executor implements terminate_timed_out_jobs
    max_runtime_seconds = DB.Column(DB.Integer, default=None)

    # free text field of arguments passed unadultered to executor
    context_args = DB.Column(DB.String(1000), default=None)

    # sge specific parameters
    sge_queue = DB.Column(DB.String(255), default=None)
    sge_num_cores = DB.Column(DB.Integer, default=None)
    sge_m_mem_free = DB.Column(DB.String(255), default=None)
    sge_j_resource = DB.Column(DB.Boolean, default=None)
    sge_project = DB.Column(DB.String(25), default=None)

    # ORM relationships
    job = DB.relationship("Job", foreign_keys=[job_id])

    def activate(self):
        self.job.executor_parameters_id = self.id
