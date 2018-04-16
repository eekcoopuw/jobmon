import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from datetime import datetime

from jobmon.sql_base import Base

logger = logging.getLogger(__name__)


class InvalidStateTransition(Exception):
    def __init__(self, model, id, old_state, new_state):
        msg = "Cannot transition {} id: {} from {} to {}".format(
            model, id, old_state, new_state)
        super(InvalidStateTransition, self).__init__(self, msg)


class JobStatus(Base):
    __tablename__ = 'job_status'

    REGISTERED = 1
    QUEUED_FOR_INSTANTIATION = 2
    INSTANTIATED = 3
    RUNNING = 4
    ERROR_RECOVERABLE = 5
    ERROR_FATAL = 6
    DONE = 7

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class JobInstanceStatus(Base):
    __tablename__ = 'job_instance_status'

    INSTANTIATED = 1
    SUBMITTED_TO_BATCH_EXECUTOR = 2
    RUNNING = 3
    ERROR = 4
    DONE = 5

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class Job(Base):
    __tablename__ = 'job'

    @classmethod
    def from_wire(cls, dct):
        return cls(dag_id=dct['dag_id'], job_id=dct['job_id'],
                   job_hash=dct['job_hash'], name=dct['name'],
                   command=dct['command'], slots=dct['slots'],
                   mem_free=dct['mem_free'], status=dct['status'],
                   num_attempts=dct['num_attempts'],
                   max_attempts=dct['max_attempts'],
                   context_args=dct['context_args'])

    def to_wire(self):
        return {'dag_id': self.dag_id, 'job_id': self.job_id,
                'name': self.name, 'job_hash': self.job_hash,
                'command': self.command, 'status': self.status,
                'slots': self.slots, 'mem_free': self.mem_free,
                'num_attempts': self.num_attempts,
                'max_attempts': self.max_attempts,
                'context_args': self.context_args}

    job_id = Column(Integer, primary_key=True)
    job_instances = relationship("JobInstance", back_populates="job")
    dag_id = Column(
        Integer,
        ForeignKey('task_dag.dag_id'))
    job_hash = Column(String(255))
    name = Column(String(255))
    command = Column(Text)
    context_args = Column(String(1000))
    slots = Column(Integer, default=1)
    mem_free = Column(Integer, default=1)
    num_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=1)
    max_runtime = Column(Integer)
    status = Column(
        Integer,
        ForeignKey('job_status.id'),
        nullable=False)
    submitted_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)

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
        self.status = JobStatus.REGISTERED
        self.num_attempts = 0
        for ji in self.job_instances:
            ji.status = JobInstanceStatus.ERROR
            new_error = JobInstanceErrorLog(description="Job RESET requested")
            ji.errors.append(new_error)

    def transition_to_error(self):
        self.transition(JobStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            logger.debug("ZZZ GIVING UP Job {}".format(self.job_id))
            self.transition(JobStatus.ERROR_FATAL)
        else:
            logger.debug("ZZZ retrying Job {}".format(self.job_id))
            self.transition(JobStatus.QUEUED_FOR_INSTANTIATION)

    def transition(self, new_state):
        self._validate_transition(new_state)
        if new_state == JobStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state
        self.status_date = datetime.utcnow()

    def _validate_transition(self, new_state):
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('Job', self.job_id, self.status,
                                         new_state)


class JobInstance(Base):
    __tablename__ = 'job_instance'

    @classmethod
    def from_wire(cls, dct):
        return cls(job_instance_id=dct['job_instance_id'],
                   workflow_run_id=dct['workflow_run_id'],
                   executor_id=dct['executor_id'],
                   job_id=dct['job_id'],
                   status=dct['status'],
                   status_date=datetime.strptime(dct['status_date'],
                                                 "%Y-%m-%dT%H:%M:%S"))

    def to_wire(self):
        time_since_status = (datetime.utcnow() - self.status_date).seconds
        return {'job_instance_id': self.job_instance_id,
                'workflow_run_id': self.workflow_run_id,
                'executor_id': self.executor_id,
                'job_id': self.job_id,
                'status': self.status,
                'status_date': self.status_date.strftime("%Y-%m-%dT%H:%M:%S"),
                'time_since_status_update': time_since_status,
                'max_runtime': self.job.max_runtime}

    job_instance_id = Column(Integer, primary_key=True)
    workflow_run_id = Column(Integer)
    executor_type = Column(String(50))
    executor_id = Column(Integer)
    job_id = Column(
        Integer,
        ForeignKey('job.job_id'),
        nullable=False)
    job = relationship("Job", back_populates="job_instances")
    usage_str = Column(String(250))
    nodename = Column(String(50))
    wallclock = Column(String(50))
    maxvmem = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    status = Column(
        Integer,
        ForeignKey('job_instance_status.id'),
        default=JobInstanceStatus.INSTANTIATED,
        nullable=False)
    submitted_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)

    errors = relationship("JobInstanceErrorLog", back_populates="job_instance")

    valid_transitions = [
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.RUNNING),

        (JobInstanceStatus.INSTANTIATED,
         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.RUNNING),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.ERROR),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.ERROR),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.DONE)]

    def transition(self, new_state):
        self._validate_transition(new_state)
        self.status = new_state
        self.status_date = datetime.utcnow()
        if new_state == JobInstanceStatus.RUNNING:
            self.job.transition(JobStatus.RUNNING)
        elif new_state == JobInstanceStatus.DONE:
            self.job.transition(JobStatus.DONE)
        elif new_state == JobInstanceStatus.ERROR:
            self.job.transition_to_error()

    def _validate_transition(self, new_state):
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('JobInstance', self.job_instance_id,
                                         self.status, new_state)


class JobInstanceErrorLog(Base):
    __tablename__ = 'job_instance_error_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = Column(DateTime, default=datetime.utcnow)
    description = Column(Text)

    job_instance = relationship("JobInstance", back_populates="errors")


class JobInstanceStatusLog(Base):
    __tablename__ = 'job_instance_status_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    status = Column(
        Integer,
        ForeignKey('job_instance_status.id'),
        nullable=False)
    status_time = Column(DateTime, default=datetime.utcnow)
