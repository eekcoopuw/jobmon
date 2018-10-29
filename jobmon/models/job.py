import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, Boolean
from sqlalchemy.orm import relationship

from datetime import datetime

from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.sql_base import Base
from jobmon.models.exceptions import InvalidStateTransition

logger = logging.getLogger(__name__)


class Job(Base):
    """The table in the database that holds all info on Jobs"""

    __tablename__ = 'job'

    @classmethod
    def from_wire(cls, dct):
        return cls(dag_id=dct['dag_id'], job_id=dct['job_id'],
                   job_hash=int(dct['job_hash']), name=dct['name'],
                   tag=dct['tag'], command=dct['command'], slots=dct['slots'],
                   mem_free=dct['mem_free'], num_cores=dct['num_cores'],
                   status=dct['status'], max_runtime_seconds=dct['max_runtime_seconds'],
                   num_attempts=dct['num_attempts'],
                   max_attempts=dct['max_attempts'],
                   context_args=dct['context_args'],
                   queue=dct['queue'], j_resource=dct['j_resource'],
                   last_nodename=dct['last_nodename'],
                   last_process_group_id=dct['last_process_group_id'])

    def to_wire(self):
        lnode, lpgid = self._last_instance_procinfo()
        return {'dag_id': self.dag_id, 'job_id': self.job_id,
                'name': self.name, 'tag': self.tag, 'job_hash': self.job_hash,
                'command': self.command, 'status': self.status,
                'slots': self.slots, 'mem_free': self.mem_free,
                'num_cores': self.num_cores,
                'max_runtime_seconds': self.max_runtime_seconds,
                'num_attempts': self.num_attempts,
                'max_attempts': self.max_attempts,
                'context_args': self.context_args,
                'queue': self.queue, 'j_resource': self.j_resource,
                'last_nodename': lnode,
                'last_process_group_id': lpgid}

    job_id = Column(Integer, primary_key=True)
    job_instances = relationship("JobInstance", back_populates="job")
    dag_id = Column(
        Integer,
        ForeignKey('task_dag.dag_id'))
    job_hash = Column(String(255), nullable=False)
    name = Column(String(255))
    tag = Column(String(255))
    command = Column(Text)
    context_args = Column(String(1000))
    queue = Column(String(255))
    slots = Column(Integer, default=None)
    mem_free = Column(String(255))
    num_cores = Column(Integer, default=None)
    j_resource = Column(Boolean, unique=False, default=False)
    max_runtime_seconds = Column(Integer, default=None)
    num_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=1)
    status = Column(
        String(1),
        ForeignKey('job_status.id'),
        nullable=False)
    submitted_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)

    last_nodename = None
    last_process_group_id = None

    valid_transitions = [
        (JobStatus.REGISTERED, JobStatus.QUEUED_FOR_INSTANTIATION),
        (JobStatus.QUEUED_FOR_INSTANTIATION, JobStatus.INSTANTIATED),
        (JobStatus.INSTANTIATED, JobStatus.RUNNING),
        (JobStatus.INSTANTIATED, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.RUNNING, JobStatus.DONE),
        (JobStatus.RUNNING, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.ERROR_FATAL),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.QUEUED_FOR_INSTANTIATION)]

    def reset(self):
        """Reset status and number of attempts on a Job"""
        self.status = JobStatus.REGISTERED
        self.num_attempts = 0
        for ji in self.job_instances:
            ji.status = JobInstanceStatus.ERROR
            new_error = JobInstanceErrorLog(description="Job RESET requested")
            ji.errors.append(new_error)

    def transition_to_error(self):
        """Transition the Job to an error state"""
        self.transition(JobStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            logger.debug("ZZZ GIVING UP Job {}".format(self.job_id))
            self.transition(JobStatus.ERROR_FATAL)
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
