import logging
from datetime import datetime

from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_status import JobStatus
from jobmon.models.exceptions import InvalidStateTransition, KillSelfTransition

logger = logging.getLogger(__name__)


class JobInstance(DB.Model):
    """The table in the database that holds all info on JobInstances"""

    __tablename__ = 'job_instance'

    @classmethod
    def from_wire(cls, dct):
        return cls(job_instance_id=dct['job_instance_id'],
                   workflow_run_id=dct['workflow_run_id'],
                   executor_id=dct['executor_id'],
                   nodename=dct['nodename'],
                   process_group_id=dct['process_group_id'],
                   job_id=dct['job_id'],
                   dag_id=dct['dag_id'],
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
            'dag_id': self.dag_id,
            'status': self.status,
            'nodename': self.nodename,
            'process_group_id': self.process_group_id,
            'status_date': self.status_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'time_since_status_update': time_since_status,
        }

    job_instance_id = DB.Column(DB.Integer, primary_key=True)
    workflow_run_id = DB.Column(DB.Integer)
    executor_type = DB.Column(DB.String(50))
    executor_id = DB.Column(DB.Integer, index=True)
    job_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('job.job_id'),
        nullable=False)
    job = DB.relationship("Job", back_populates="job_instances")
    dag_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('task_dag.dag_id'),
        index=True,
        nullable=False)
    dag = DB.relationship("TaskDagMeta")
    usage_str = DB.Column(DB.String(250))
    nodename = DB.Column(DB.String(50))
    process_group_id = DB.Column(DB.Integer)
    wallclock = DB.Column(DB.String(50))
    maxrss = DB.Column(DB.String(50))
    cpu = DB.Column(DB.String(50))
    io = DB.Column(DB.String(50))
    status = DB.Column(
        DB.String(1),
        DB.ForeignKey('job_instance_status.id'),
        default=JobInstanceStatus.INSTANTIATED,
        nullable=False)
    submitted_date = DB.Column(DB.DateTime, default=datetime.utcnow)
    status_date = DB.Column(DB.DateTime, default=datetime.utcnow)
    report_by_date = DB.Column(
        DB.DateTime,
        default=func.UTC_TIMESTAMP())

    errors = DB.relationship("JobInstanceErrorLog",
                             back_populates="job_instance")

    valid_transitions = [
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.RUNNING),

        (JobInstanceStatus.INSTANTIATED,
         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.NO_EXECUTOR_ID),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.RUNNING),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.LOST_TRACK),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.ERROR),

        (JobInstanceStatus.LOST_TRACK, JobInstanceStatus.ERROR),

        (JobInstanceStatus.LOST_TRACK, JobInstanceStatus.UNKNOWN_ERROR),

        (JobInstanceStatus.LOST_TRACK, JobInstanceStatus.RESOURCE_ERROR),

        (JobInstanceStatus.LOST_TRACK, JobInstanceStatus.DONE),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.ERROR),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.LOST_TRACK),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.DONE)
    ]

    untimely_transitions = [
        (JobInstanceStatus.RUNNING,
         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
    ]

    kill_self_states = [JobInstanceStatus.NO_EXECUTOR_ID,
                        JobInstanceStatus.UNKNOWN_ERROR,
                        JobInstanceStatus.RESOURCE_ERROR]

    def transition(self, new_state):
        """Transition the JobInstance status"""
        # if the transition is timely, move to new state. Otherwise do nothing
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            self.status = new_state
            self.status_date = datetime.utcnow()
            if new_state == JobInstanceStatus.RUNNING:
                self.job.transition(JobStatus.RUNNING)
            elif new_state == JobInstanceStatus.DONE:
                self.job.transition(JobStatus.DONE)
            elif new_state == JobInstanceStatus.ERROR:
                self.job.transition_to_error()
            elif new_state == JobInstanceStatus.NO_EXECUTOR_ID:
                self.job.transition_to_error()

    def _validate_transition(self, new_state):
        """Ensure the JobInstance status transition is valid"""
        if self.status in self.__class__.kill_self_states and \
                new_state is JobInstanceStatus.RUNNING:
            raise KillSelfTransition('JobInstance', self.job_instance_id,
                                     self.status, new_state)
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('JobInstance', self.job_instance_id,
                                         self.status, new_state)

    def _is_timely_transition(self, new_state):
        """Check if the transition is invalid due to a race condition"""

        if (self.status, new_state) in self.__class__.untimely_transitions:
            msg = str(InvalidStateTransition(
                'JobInstance', self.job_instance_id, self.status, new_state))
            msg += ". This is an untimely transition likely caused by a race "\
                   " condition between the UGE scheduler and the job instance"\
                   " factory which logs the UGE id on the job instance."
            logger.warning(msg)
            return False
        else:
            return True
