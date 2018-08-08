import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from datetime import datetime

from jobmon.models.sql_base import Base
from jobmon.models.job_instance_status import JobInstanceStatus

logger = logging.getLogger(__name__)


class JobInstance(Base):
    __tablename__ = 'job_instance'

    @classmethod
    def from_wire(cls, dct):
        return cls(job_instance_id=dct['job_instance_id'],
                   workflow_run_id=dct['workflow_run_id'],
                   executor_id=dct['executor_id'],
                   nodename=dct['nodename'],
                   process_group_id=dct['process_group_id'],
                   job_id=dct['job_id'],
                   status=dct['status'],
                   status_date=datetime.strptime(dct['status_date'],
                                                 "%Y-%m-%dT%H:%M:%S"))

    def to_wire(self):
        time_since_status = (datetime.utcnow() - self.status_date).seconds
        return {
            'job_instance_id': self.job_instance_id,
            'workflow_run_id': self.workflow_run_id,
            'executor_id': self.executor_id,
            'job_id': self.job_id,
            'status': self.status,
            'nodename': self.nodename,
            'process_group_id': self.process_group_id,
            'status_date': self.status_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'time_since_status_update': time_since_status,
        }

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
    process_group_id = Column(Integer)
    wallclock = Column(String(50))
    maxvmem = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    status = Column(
        String(1),
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

    def register(self, requester, executor_type):
        rc, response = requester.send_request(
            app_route='/add_job_instance',
            message={'job_id': str(self.job.job_id),
                     'executor_type': executor_type},
            request_type='post')
        self.job_instance_id = response['job_instance_id']
        return self.job_instance_id

    def assign_executor_id(self, requester, executor_id):
        requester.send_request(
            app_route='/log_executor_id',
            message={'job_instance_id': str(self.job_instance_id),
                     'executor_id': str(executor_id)},
            request_type='post')

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
