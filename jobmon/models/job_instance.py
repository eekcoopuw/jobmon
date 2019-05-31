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

    def to_wire_as_executor_job_instance(self):
        return {
            'job_instance_id': self.job_instance_id,
            'executor_id': self.executor_id
        }

    # TODO: figure out what should be passed to workflow_run when called during
    # resume
    def to_wire(self):
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
        }

    job_instance_id = DB.Column(DB.Integer, primary_key=True)
    workflow_run_id = DB.Column(DB.Integer)
    executor_type = DB.Column(DB.String(50))
    executor_id = DB.Column(DB.Integer, index=True)
    job_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('job.job_id'),
        nullable=False)
    dag_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('task_dag.dag_id'),
        index=True,
        nullable=False)
    executor_parameter_set_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('executor_parameter_set.id'),
        nullable=False)

    # usage
    usage_str = DB.Column(DB.String(250))
    nodename = DB.Column(DB.String(50))
    process_group_id = DB.Column(DB.Integer)
    wallclock = DB.Column(DB.String(50))
    maxrss = DB.Column(DB.String(50))
    cpu = DB.Column(DB.String(50))
    io = DB.Column(DB.String(50))

    # status/state
    status = DB.Column(
        DB.String(1),
        DB.ForeignKey('job_instance_status.id'),
        default=JobInstanceStatus.INSTANTIATED,
        nullable=False)
    submitted_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    report_by_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())

    # ORM relationships
    job = DB.relationship("Job", back_populates="job_instances")
    dag = DB.relationship("TaskDagMeta")
    errors = DB.relationship("JobInstanceErrorLog",
                             back_populates="job_instance")
    executor_parameter_set = DB.relationship("ExecutorParameterSet")

    # finite state machine transition information
    valid_transitions = [
        # job instance is submitted normally (happy path)
        (JobInstanceStatus.INSTANTIATED,
         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        # job instance submission hit weird bug and didn't get an executor_id
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.NO_EXECUTOR_ID),

        # job instance logs running before submitted due to race condition
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.RUNNING),

        # job instance logs running after submission to batch (happy path)
        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.RUNNING),

        # job instance disappeared from executor heartbeat and never logged
        # running. The executor has no accounting of why it died
        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.UNKNOWN_ERROR),

        # job instance disappeared from executor heartbeat and never logged
        # running. The executor discovered a resource error exit status.
        # This seems unlikely but is valid for the purposes of the FSM
        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.RESOURCE_ERROR),

        # job instance hits an application error (happy path)
        (JobInstanceStatus.RUNNING, JobInstanceStatus.ERROR),

        # job instance stops logging heartbeats. reconciler can't find an exit
        # status
        (JobInstanceStatus.RUNNING, JobInstanceStatus.UNKNOWN_ERROR),

        # 1) job instance stops logging heartbeats. reconciler discovers a
        # resource error.
        # 2) worker node detects a resource error
        (JobInstanceStatus.RUNNING, JobInstanceStatus.RESOURCE_ERROR),

        # job instance finishes normally (happy path)
        (JobInstanceStatus.RUNNING, JobInstanceStatus.DONE)
    ]

    untimely_transitions = [

        # job instance logs running before the executor logs submitted due to
        # race condition. this is unlikely but happens and is valid for the
        # purposes of the FSM
        (JobInstanceStatus.RUNNING,
         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        # job instance stops logging heartbeats and reconciler is looking for
        # remote exit status but can't find it so logs an unknown error.
        # Job finishes with an application error. We can't update state because
        # the job may already be running again due to a race with the JIF
        (JobInstanceStatus.ERROR, JobInstanceStatus.UNKNOWN_ERROR),

        # job instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to finish gracefully but reconciler won the race
        (JobInstanceStatus.UNKNOWN_ERROR, JobInstanceStatus.DONE),

        # job instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to report an application error but cant' because
        # the job could be running again alread and we don't want to update job
        # state
        (JobInstanceStatus.UNKNOWN_ERROR, JobInstanceStatus.ERROR),

        # job instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to report a resource error but cant' because
        # the job could be running again alread and we don't want to update job
        # state
        (JobInstanceStatus.UNKNOWN_ERROR, JobInstanceStatus.RESOURCE_ERROR),

        # job instance stops logging heartbeats and reconciler can't find exit
        # status. Worker reports a resource error before reconciler logs an
        # unknown error.
        (JobInstanceStatus.RESOURCE_ERROR, JobInstanceStatus.UNKNOWN_ERROR),

        # job instance stops logging heartbeats and reconciler is looking for
        # remote exit status but can't find it so logs an unknown error.
        # The worker finishes gracefully before reconciler can log an unknown
        # error
        (JobInstanceStatus.DONE, JobInstanceStatus.UNKNOWN_ERROR)
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
            elif new_state == JobInstanceStatus.UNKNOWN_ERROR:
                self.job.transition_to_error()
            elif new_state == JobInstanceStatus.RESOURCE_ERROR:
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
