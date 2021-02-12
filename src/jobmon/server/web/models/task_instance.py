from jobmon.serializers import SerializeExecutorTaskInstance
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition, KillSelfTransition
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus

from sqlalchemy.sql import func

import structlog as logging

logger = logging.getLogger(__name__)


class TaskInstance(DB.Model):

    __tablename__ = "task_instance"

    def to_wire_as_executor_task_instance(self):
        return SerializeExecutorTaskInstance.to_wire(self.id,
                                                     self.workflow_run_id,
                                                     self.executor_id)

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_run_id = DB.Column(DB.Integer)
    executor_type = DB.Column(DB.String(50))
    executor_id = DB.Column(DB.Integer, index=True)
    task_id = DB.Column(DB.Integer, DB.ForeignKey('task.id'))
    executor_parameter_set_id = DB.Column(
        DB.Integer, DB.ForeignKey('executor_parameter_set.id'))

    # usage
    nodename = DB.Column(DB.String(150))
    process_group_id = DB.Column(DB.Integer)
    usage_str = DB.Column(DB.String(250))
    wallclock = DB.Column(DB.String(50))
    maxrss = DB.Column(DB.String(50))
    maxpss = DB.Column(DB.String(50))
    cpu = DB.Column(DB.String(50))
    io = DB.Column(DB.String(50))

    # status/state
    status = DB.Column(DB.String(1), DB.ForeignKey('task_instance_status.id'),
                       default=TaskInstanceStatus.INSTANTIATED)
    submitted_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())
    report_by_date = DB.Column(DB.DateTime, default=func.now())

    # ORM relationships
    task = DB.relationship("Task", back_populates="task_instances")
    errors = DB.relationship("TaskInstanceErrorLog",
                             back_populates="task_instance")
    executor_parameter_set = DB.relationship("ExecutorParameterSet")

    # finite state machine transition information
    valid_transitions = [
        # task instance is submitted normally (happy path)
        (TaskInstanceStatus.INSTANTIATED,
         TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        # task instance submission hit weird bug and didn't get an executor_id
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.NO_EXECUTOR_ID),

        # task instance is mid submission and a new workflow run starts and
        # tells it to die
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.KILL_SELF),

        # task instance logs running before submitted due to race condition
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.RUNNING),

        # task instance logs running after submission to batch (happy path)
        (TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         TaskInstanceStatus.RUNNING),

        # task instance disappeared from executor heartbeat and never logged
        # running. The executor has no accounting of why it died
        (TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         TaskInstanceStatus.UNKNOWN_ERROR),

        # task instance disappeared from executor heartbeat and never logged
        # running. The executor discovered a resource error exit status.
        # This seems unlikely but is valid for the purposes of the FSM
        (TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         TaskInstanceStatus.RESOURCE_ERROR),

        # task instance is submitted to the batch executor waiting to start
        # running. new workflow run is created and this task is told to kill
        # itself
        (TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         TaskInstanceStatus.KILL_SELF),

        # task instance hits an application error (happy path)
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.ERROR),

        # task instance stops logging heartbeats. reconciler can't find an exit
        # status
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.UNKNOWN_ERROR),

        # 1) task instance stops logging heartbeats. reconciler discovers a
        # resource error.
        # 2) worker node detects a resource error
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.RESOURCE_ERROR),

        # task instance is running. another workflow run starts and tells it to
        # die
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.KILL_SELF),

        # task instance finishes normally (happy path)
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.DONE),

        # allow task instance to transit to F to immediately fail the task
        (TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR, TaskInstanceStatus.ERROR_FATAL),
    ]

    untimely_transitions = [

        # task instance logs running before the executor logs submitted due to
        # race condition. this is unlikely but happens and is valid for the
        # purposes of the FSM
        (TaskInstanceStatus.RUNNING,
         TaskInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        # task instance stops logging heartbeats and reconciler is looking for
        # remote exit status but can't find it so logs an unknown error. task
        # finishes with an application error. We can't update state because
        # the task may already be running again due to a race with the JIF
        (TaskInstanceStatus.ERROR, TaskInstanceStatus.UNKNOWN_ERROR),

        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to finish gracefully but reconciler won the race
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.DONE),

        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to report an application error but cant' because
        # the task could be running again alread and we don't want to update
        # task state
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.ERROR),

        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker tries to report a resource error but cant' because
        # the task could be running again alread and we don't want to update
        # task state
        (TaskInstanceStatus.UNKNOWN_ERROR, TaskInstanceStatus.RESOURCE_ERROR),

        # task instance stops logging heartbeats and reconciler can't find exit
        # status. Worker reports a resource error before reconciler logs an
        # unknown error.
        (TaskInstanceStatus.RESOURCE_ERROR, TaskInstanceStatus.UNKNOWN_ERROR),

        # task instance stops logging heartbeats and reconciler is looking for
        # remote exit status but can't find it so logs an unknown error.
        # The worker finishes gracefully before reconciler can log an unknown
        # error
        (TaskInstanceStatus.DONE, TaskInstanceStatus.UNKNOWN_ERROR),

        # task is reset by workflow resume and worker finishes gracefully but
        # resume won the race
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.DONE),

        # task is reset by workflow resume and worker finishes with application
        # error but resume won the race
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.ERROR),

        # task is reset by workflow resume and reconciler or worker node
        # discovers resource error, but resume won the race
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.RESOURCE_ERROR)
    ]

    kill_self_states = [TaskInstanceStatus.NO_EXECUTOR_ID,
                        TaskInstanceStatus.UNKNOWN_ERROR,
                        TaskInstanceStatus.RESOURCE_ERROR,
                        TaskInstanceStatus.KILL_SELF]

    error_states = [TaskInstanceStatus.NO_EXECUTOR_ID,
                    TaskInstanceStatus.ERROR,
                    TaskInstanceStatus.UNKNOWN_ERROR,
                    TaskInstanceStatus.RESOURCE_ERROR,
                    TaskInstanceStatus.KILL_SELF]

    def transition(self, new_state):
        """Transition the TaskInstance status"""
        # if the transition is timely, move to new state. Otherwise do nothing
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            self.status = new_state
            self.status_date = func.now()
            if new_state == TaskInstanceStatus.RUNNING:
                self.task.transition(TaskStatus.RUNNING)
            elif new_state == TaskInstanceStatus.DONE:
                self.task.transition(TaskStatus.DONE)
            elif new_state in self.error_states:
                self.task.transition_after_task_instance_error(new_state)
            elif new_state == TaskInstanceStatus.ERROR_FATAL:
                # if the task instance is F, the task status should be F too
                self.task.transition(TaskStatus.ERROR_RECOVERABLE)
                self.task.transition(TaskStatus.ERROR_FATAL)

    def _validate_transition(self, new_state):
        """Ensure the TaskInstance status transition is valid"""
        if self.status in self.kill_self_states and \
                new_state is TaskInstanceStatus.RUNNING:
            raise KillSelfTransition('TaskInstance', self.id, self.status,
                                     new_state)
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('TaskInstance', self.id, self.status,
                                         new_state)

    def _is_timely_transition(self, new_state):
        """Check if the transition is invalid due to a race condition"""

        if (self.status, new_state) in self.__class__.untimely_transitions:
            msg = str(InvalidStateTransition(
                'TaskInstance', self.id, self.status, new_state))
            msg += (
                ". This is an untimely transition likely caused by a race "
                " condition between the UGE scheduler and the task instance"
                " factory which logs the UGE id on the task instance.")
            logger.warning(msg)
            return False
        else:
            return True
