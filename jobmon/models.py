from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


Base = declarative_base()


class InvalidStateTransition(Exception):
    def __init__(self, old_state, new_state):
        msg = "Cannot transition {} to {}".format(old_state, new_state)
        super().__init__(self, msg)


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
    RUNNING = 2
    ERROR = 3
    DONE = 4

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class JobDag(Base):
    __tablename__ = 'job_dag'

    dag_id = Column(Integer, primary_key=True)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime, default=func.now())


class Job(Base):
    __tablename__ = 'job'

    job_id = Column(Integer, primary_key=True)
    job_instances = relationship("JobInstance", back_populates="job")
    dag_id = Column(
        Integer,
        ForeignKey('job_dag.dag_id'))
    name = Column(String(150))
    runfile = Column(String(150))
    args = Column(String(1000))
    num_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=1)
    status = Column(
        Integer,
        ForeignKey('job_status.id'),
        nullable=False)
    submitted_date = Column(DateTime, default=func.now())

    valid_transitions = [
        (JobStatus.REGISTERED, JobStatus.QUEUED_FOR_INSTANTIATION),
        (JobStatus.QUEUED_FOR_INSTANTIATION, JobStatus.INSTANTIATED),
        (JobStatus.INSTANTIATED, JobStatus.RUNNING),
        (JobStatus.RUNNING, JobStatus.DONE),
        (JobStatus.RUNNING, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.ERROR_FATAL),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.QUEUED_FOR_INSTANTIATION)]

    def transition_to_error(self):
        self.transition(JobStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            self.transition(JobStatus.ERROR_FATAL)
        else:
            self.transition(JobStatus.QUEUED_FOR_INSTANTIATION)

    def transition(self, new_state):
        self._validate_transition(new_state)
        if new_state == JobStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state

    def _validate_transition(self, new_state):
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition(self.status, new_state)


class JobInstance(Base):
    __tablename__ = 'job_instance'

    job_instance_id = Column(Integer, primary_key=True)
    job_id = Column(
        Integer,
        ForeignKey('job.job_id'),
        nullable=False,
        primary_key=True)
    job = relationship("Job", back_populates="job_instances")
    job_instance_type = Column(String(50))
    usage_str = Column(String(250))
    wallclock = Column(String(50))
    maxvmem = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    status = Column(
        Integer,
        ForeignKey('job_instance_status.id'),
        default=JobInstanceStatus.INSTANTIATED,
        nullable=False)
    submitted_date = Column(DateTime, default=func.now())

    valid_transitions = [
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.RUNNING),
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.ERROR),
        (JobInstanceStatus.RUNNING, JobInstanceStatus.ERROR),
        (JobInstanceStatus.RUNNING, JobInstanceStatus.ERROR)]

    def transition(self, new_state):
        self._validate_transition(new_state)
        self.status = new_state
        if new_state == JobInstanceStatus.RUNNING:
            self.job.transition(JobStatus.RUNNING)
        elif new_state == JobInstanceStatus.DONE:
            self.job.transition(JobStatus.DONE)
        elif new_state == JobInstanceStatus.ERROR:
            self.job.transition_to_error()

    def _validate_transition(self, new_state):
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition(self.status, new_state)


class JobInstanceErrorLog(Base):
    __tablename__ = 'job_instance_error_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = Column(DateTime, default=func.now())
    description = Column(String(1000), nullable=False)


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
    status_time = Column(DateTime, default=func.now())
