"""Task Instance Database Table."""
from functools import partial
from typing import Tuple

from sqlalchemy.sql import func
from werkzeug.local import LocalProxy

from jobmon.serializers import SerializeTaskInstance
from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import (
    InvalidStateTransition,
    KillSelfTransition,
)
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


class TaskInstance(DB.Model):
    """Task Instance Database Table."""

    __tablename__ = "task_instance"

    def to_wire_as_distributor_task_instance(self) -> Tuple:
        """Serialize task instance object."""
        return SerializeTaskInstance.to_wire(
            self.id, self.task_id, self.workflow_run_id, self.task.workflow_id,
            self.status, self.distributor_id, self.cluster_id,
            self.array_id, self.array_batch_num, self.array_step_id, self.subtask_id
        )

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_run_id = DB.Column(DB.Integer)
    array_id = DB.Column(DB.Integer, DB.ForeignKey("array.id"), default=None)
    cluster_id = DB.Column(DB.Integer, DB.ForeignKey("cluster.id"))
    distributor_id = DB.Column(DB.Integer, index=True)
    task_id = DB.Column(DB.Integer, DB.ForeignKey("task.id"))
    task_resources_id = DB.Column(DB.Integer, DB.ForeignKey("task_resources.id"))
    array_batch_num = DB.Column(DB.Integer)
    array_step_id = DB.Column(DB.Integer)
    subtask_id = DB.Column(DB.String(100))

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
    status = DB.Column(
        DB.String(1),
        DB.ForeignKey("task_instance_status.id"),
        default=TaskInstanceStatus.INSTANTIATED,
    )
    submitted_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())
    report_by_date = DB.Column(DB.DateTime, default=func.now())

    # ORM relationships
    task = DB.relationship("Task", back_populates="task_instances")
    errors = DB.relationship("TaskInstanceErrorLog", back_populates="task_instance")
    task_resources = DB.relationship("TaskResources")

    # finite state machine transition information
    valid_transitions = [
        # task instance is moved from queued to instantiated by distributor
        (TaskInstanceStatus.QUEUED, TaskInstanceStatus.INSTANTIATED),
        # task instance is queued and waiting to instantiate when a new workflow run starts and
        # tells it to die
        (TaskInstanceStatus.QUEUED, TaskInstanceStatus.KILL_SELF),
        # task instance is launched by distributor
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.LAUNCHED),
        # task instance submission hit weird bug and didn't get an distributor_id
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.NO_DISTRIBUTOR_ID),
        # task instance is mid submission and a new workflow run starts and
        # tells it to die
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.KILL_SELF),
        # task instance logs running before submitted due to race condition
        (TaskInstanceStatus.INSTANTIATED, TaskInstanceStatus.RUNNING),
        # task instance running after transitioning from launched
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.RUNNING),
        # task instance disappeared from distributor heartbeat and never logged
        # running. The distributor has no accounting of why it died
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.UNKNOWN_ERROR),
        # task instance disappeared from distributor heartbeat and never logged
        # running. The distributor discovered a resource error exit status.
        # This seems unlikely but is valid for the purposes of the FSM
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.RESOURCE_ERROR),
        # task instance is submitted to the batch distributor waiting to launch.
        # new workflow run is created and this task is told to kill
        # itself
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.KILL_SELF),
        # allow task instance to transit to F to immediately fail the task if there is an env
        # mismatch
        (TaskInstanceStatus.LAUNCHED, TaskInstanceStatus.ERROR_FATAL),
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
    ]

    untimely_transitions = [
        # task instance logs running before the distributor logs submitted due to
        # race condition. this is unlikely but happens and is valid for the
        # purposes of the FSM
        (TaskInstanceStatus.RUNNING, TaskInstanceStatus.LAUNCHED),
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
        (TaskInstanceStatus.KILL_SELF, TaskInstanceStatus.RESOURCE_ERROR),
    ]

    kill_self_states = [
        TaskInstanceStatus.NO_DISTRIBUTOR_ID,
        TaskInstanceStatus.UNKNOWN_ERROR,
        TaskInstanceStatus.RESOURCE_ERROR,
        TaskInstanceStatus.KILL_SELF,
    ]

    error_states = [
        TaskInstanceStatus.NO_DISTRIBUTOR_ID,
        TaskInstanceStatus.ERROR,
        TaskInstanceStatus.UNKNOWN_ERROR,
        TaskInstanceStatus.RESOURCE_ERROR,
        TaskInstanceStatus.KILL_SELF,
    ]

    def transition(self, new_state: str) -> None:
        """Transition the TaskInstance status."""
        # if the transition is timely, move to new state. Otherwise do nothing
        bind_to_logger(
            workflow_run_id=self.workflow_run_id,
            task_id=self.task_id,
            task_instance_id=self.id,
        )
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            logger.info(f"Transitioning task_instance from {self.status} to {new_state}")
            self.status = new_state
            self.status_date = func.now()
            if new_state == TaskInstanceStatus.QUEUED:
                self.task.transition(TaskStatus.QUEUED)
            if new_state == TaskInstanceStatus.INSTANTIATED:
                self.task.transition(TaskStatus.INSTANTIATING)
            if new_state == TaskInstanceStatus.LAUNCHED:
                self.task.transition(TaskStatus.LAUNCHED)
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

    def _validate_transition(self, new_state: str) -> None:
        """Ensure the TaskInstance status transition is valid."""
        if (
            self.status in self.kill_self_states
            and new_state is TaskInstanceStatus.RUNNING
        ):
            raise KillSelfTransition("TaskInstance", self.id, self.status, new_state)
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition(
                "TaskInstance", self.id, self.status, new_state
            )

    def _is_timely_transition(self, new_state: str) -> bool:
        """Check if the transition is invalid due to a race condition."""
        if (self.status, new_state) in self.__class__.untimely_transitions:
            msg = str(
                InvalidStateTransition("TaskInstance", self.id, self.status, new_state)
            )
            msg += (
                ". This is an untimely transition likely caused by a race "
                " condition between the UGE distributor and the task instance"
                " factory which logs the UGE id on the task instance."
            )
            logger.warning(msg)
            return False
        else:
            return True
