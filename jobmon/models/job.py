import logging
from datetime import datetime

from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.serializers import SerializeExecutorJob, SerializeSwarmJob

logger = logging.getLogger(__name__)


class Job(DB.Model):
    """The table in the database that holds all info on Jobs"""

    __tablename__ = 'job'
    __table_args__ = (
        DB.Index("ix_dag_id_status_date", "dag_id", "status_date"),)

    def to_wire_as_executor_job(self):
        lnode, lpgid = self._last_instance_procinfo()
        serialized = SerializeExecutorJob.to_wire(
            dag_id=self.dag_id,
            job_id=self.job_id,
            name=self.name,
            job_hash=self.job_hash,
            command=self.command,
            status=self.status,
            max_runtime_seconds=(
                self.executor_parameter_set.max_runtime_seconds),
            context_args=self.executor_parameter_set.context_args,
            resource_scales=self.executor_parameter_set.resource_scales,
            queue=self.executor_parameter_set.queue,
            num_cores=self.executor_parameter_set.num_cores,
            m_mem_free=self.executor_parameter_set.m_mem_free,
            j_resource=self.executor_parameter_set.j_resource,
            last_nodename=lnode,
            last_process_group_id=lpgid,
            hard_limits=self.executor_parameter_set.hard_limits)
        return serialized

    def to_wire_as_swarm_job(self):
        serialized = SerializeSwarmJob.to_wire(
            job_id=self.job_id,
            job_hash=self.job_hash,
            status=self.status)
        return serialized

    # identifiers
    job_id = DB.Column(DB.Integer, primary_key=True)
    dag_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('task_dag.dag_id'))
    job_hash = DB.Column(DB.String(255), nullable=False)
    name = DB.Column(DB.String(255))
    tag = DB.Column(DB.String(255))

    # execution info
    command = DB.Column(DB.Text)
    executor_parameter_set_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('executor_parameter_set.id'),
        default=None)

    # status/state
    num_attempts = DB.Column(DB.Integer, default=0)
    max_attempts = DB.Column(DB.Integer, default=1)
    status = DB.Column(
        DB.String(1),
        DB.ForeignKey('job_status.id'),
        nullable=False)
    submitted_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP(),
                            index=True)

    # ORM relationships
    job_instances = DB.relationship("JobInstance", back_populates="job")
    executor_parameter_set = DB.relationship(
        "ExecutorParameterSet", foreign_keys=[executor_parameter_set_id])

    # Materialized attributes, derived during to_wire() only. Not represented
    # in the database model
    last_nodename = None
    last_process_group_id = None

    valid_transitions = [
        (JobStatus.REGISTERED, JobStatus.QUEUED_FOR_INSTANTIATION),
        (JobStatus.ADJUSTING_RESOURCES, JobStatus.QUEUED_FOR_INSTANTIATION),
        (JobStatus.QUEUED_FOR_INSTANTIATION, JobStatus.INSTANTIATED),
        (JobStatus.INSTANTIATED, JobStatus.RUNNING),
        (JobStatus.INSTANTIATED, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.RUNNING, JobStatus.DONE),
        (JobStatus.RUNNING, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.ADJUSTING_RESOURCES),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.QUEUED_FOR_INSTANTIATION),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.ERROR_FATAL)]

    def reset(self):
        """Reset status and number of attempts on a Job"""
        self.status = JobStatus.REGISTERED
        self.num_attempts = 0
        for ji in self.job_instances:
            ji.status = JobInstanceStatus.ERROR
            new_error = JobInstanceErrorLog(description="Job RESET requested")
            ji.errors.append(new_error)

    def transition_after_job_instance_error(self, job_instance_error_state):
        """Transition the Job to an error state"""
        self.transition(JobStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            logger.debug("ZZZ GIVING UP Job {}".format(self.job_id))
            self.transition(JobStatus.ERROR_FATAL)
        else:
            if job_instance_error_state == JobInstanceStatus.RESOURCE_ERROR:
                self.transition(JobStatus.ADJUSTING_RESOURCES)
            else:
                logger.debug("ZZZ retrying Job {}".format(self.job_id))
                self.transition(JobStatus.QUEUED_FOR_INSTANTIATION)

    def transition(self, new_state):
        """Transition the Job to a new state"""
        self._validate_transition(new_state)
        if new_state == JobStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state
        self.status_date = datetime.utcnow()

    def _last_instance_procinfo(self):
        """Retrieve all process information on the last run of this Job"""
        if self.job_instances:
            last_ji = max(self.job_instances, key=lambda i: i.job_instance_id)
            return (last_ji.nodename, last_ji.process_group_id)
        else:
            return (None, None)

    def _validate_transition(self, new_state):
        """Ensure the Job state transition is valid"""
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('Job', self.job_id, self.status,
                                         new_state)

    def __hash__(self):
        """Return the unique id of this Job"""
        return self.job_id

    def __eq__(self, other):
        """Jobs are equal if they have the same id"""
        return self.job_id == other.job_id
