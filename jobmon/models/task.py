import logging
from datetime import datetime

from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.task_instance_error_log import TaskInstanceErrorLog
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.models.task_status import TaskStatus


logger = logging.getLogger(__name__)


class Task(DB.Model):

    __tablename__ = "task"

    def to_wire_as_executor_task(self):
        pass

    def to_wire_as_swarm_task(self):
        pass

    id = DB.Column(DB.Integer, primary_key=True, nullable=False)
    # TODO: add foreign key to workflow
    workflow_id = DB.Column(DB.Integer, nullable=False)
    node_id = DB.Column(DB.Integer, DB.ForeignKey('node.id'), nullable=False)
    task_arg_hash = DB.Column(DB.Integer, nullable=False)
    name = DB.Column(DB.String(255))
    command = DB.Column(DB.Text)
    executor_parameter_set_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('executor_parameter_set.id'),
        default=None)
    num_attempts = DB.Column(DB.Integer, default=0)
    max_attempts = DB.Column(DB.Integer, default=1)
    # TODO: add foreign key to task status
    status = DB.Column(
        DB.String(1),
        DB.ForeignKey('task_status.id'),
        nullable=False)
    submitted_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP(),
                            index=True)

    # ORM relationships
    task_instances = DB.relationship("TaskInstance", back_populates="task")
    executor_parameter_set = DB.relationship(
        "ExecutorParameterSet", foreign_keys=[executor_parameter_set_id])

    # Finite state machine
    valid_transitions = [
        (TaskStatus.REGISTERED, TaskStatus.QUEUED_FOR_INSTANTIATION),
        (TaskStatus.ADJUSTING_RESOURCES, TaskStatus.QUEUED_FOR_INSTANTIATION),
        (TaskStatus.QUEUED_FOR_INSTANTIATION, TaskStatus.INSTANTIATED),
        (TaskStatus.INSTANTIATED, TaskStatus.RUNNING),
        (TaskStatus.INSTANTIATED, TaskStatus.ERROR_RECOVERABLE),
        (TaskStatus.RUNNING, TaskStatus.DONE),
        (TaskStatus.RUNNING, TaskStatus.ERROR_RECOVERABLE),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.ADJUSTING_RESOURCES),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.QUEUED_FOR_INSTANTIATION),
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.ERROR_FATAL)]

    def reset(self):
        """Reset status and number of attempts on a Task"""
        self.status = TaskStatus.REGISTERED
        self.num_attempts = 0

        # TODO: figure out if we should set command here or not.
        for ti in self.task_instances:
            ti.status = TaskInstanceStatus.ERROR
            new_error = TaskInstanceErrorLog(
                description="Task RESET requested")
            ti.errors.append(new_error)

    def transition(self, new_state):
        """Transition the Task to a new state"""
        self._validate_transition(new_state)
        if new_state == TaskStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state

        self.status_date = datetime.utcnow()

    def _last_instance_procinfo(self):
        """Retrieve all process information on the last run of this Task"""
        if self.task_instances:
            last_ti = max(self.task_instances, key=lambda i: i.id)
            return (last_ti.nodename, last_ti.process_group_id)
        else:
            return (None, None)

    def _validate_transition(self, new_state):
        """Ensure the Job state transition is valid"""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition('Task', self.id, self.status,
                                         new_state)
