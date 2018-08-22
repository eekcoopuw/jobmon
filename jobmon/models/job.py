import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from datetime import datetime

from jobmon.models.job_status import JobStatus
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance_error_log import JobInstanceErrorLog
from jobmon.models.sql_base import Base

logger = logging.getLogger(__name__)


class InvalidStateTransition(Exception):
    def __init__(self, model, id, old_state, new_state):
        msg = "Cannot transition {} id: {} from {} to {}".format(
            model, id, old_state, new_state)
        super(InvalidStateTransition, self).__init__(self, msg)


class Job(Base):
    """The table in the database that holds all info on Jobs"""
    __tablename__ = 'job'

    @classmethod
    def from_wire(cls, dct):
        return cls(dag_id=dct['dag_id'], job_id=dct['job_id'],
                   job_hash=int(dct['job_hash']), name=dct['name'],
                   tag=dct['tag'], command=dct['command'], slots=dct['slots'],
                   mem_free=dct['mem_free'], status=dct['status'],
                   num_attempts=dct['num_attempts'],
                   max_attempts=dct['max_attempts'],
                   context_args=dct['context_args'],
                   last_nodename=dct['last_nodename'],
                   last_process_group_id=dct['last_process_group_id'])

    def to_wire(self):
        lnode, lpgid = self._last_instance_procinfo()
        return {'dag_id': self.dag_id, 'job_id': self.job_id,
                'name': self.name, 'tag': self.tag, 'job_hash': self.job_hash,
                'command': self.command, 'status': self.status,
                'slots': self.slots, 'mem_free': self.mem_free,
                'num_attempts': self.num_attempts,
                'max_attempts': self.max_attempts,
                'context_args': self.context_args,
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
    slots = Column(Integer, default=1)
    mem_free = Column(Integer, default=1)
    num_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=1)
    max_runtime = Column(Integer)
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
