"""Task Table for the Database."""
from functools import partial

from sqlalchemy.sql import func
from werkzeug.local import LocalProxy

from jobmon.serializers import SerializeSwarmTask, SerializeTask
from jobmon.server.web.log_config import bind_to_logger, get_logger
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_status import TaskStatus


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


class Task(DB.Model):
    """Task Database object."""

    __tablename__ = "task"

    def to_wire_as_distributor_task(self) -> tuple:
        """Serialize executor task object."""
        serialized = SerializeTask.to_wire(
            task_id=self.id,
            workflow_id=self.workflow_id,
            node_id=self.node_id,
            task_args_hash=self.task_args_hash,
            name=self.name,
            command=self.command,
            status=self.status,
            queue_id=self.task_resources.queue_id,
            requested_resources=self.task_resources.requested_resources,
        )
        return serialized

    def to_wire_as_swarm_task(self) -> tuple:
        """Serialize swarm task."""
        serialized = SerializeSwarmTask.to_wire(task_id=self.id, status=self.status)
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey("workflow.id"))
    node_id = DB.Column(DB.Integer, DB.ForeignKey("node.id"))
    task_args_hash = DB.Column(DB.Integer)
    name = DB.Column(DB.String(255))
    command = DB.Column(DB.Text)
    task_resources_id = DB.Column(
        DB.Integer, DB.ForeignKey("task_resources.id"), default=None
    )
    num_attempts = DB.Column(DB.Integer, default=0)
    max_attempts = DB.Column(DB.Integer, default=1)
    resource_scales = DB.Column(DB.String(1000), default=None)
    fallback_queues = DB.Column(DB.String(1000), default=None)
    status = DB.Column(DB.String(1), DB.ForeignKey("task_status.id"))
    submitted_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())

    # ORM relationships
    task_instances = DB.relationship("TaskInstance", back_populates="task")
    task_resources = DB.relationship("TaskResources", foreign_keys=[task_resources_id])

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
        (TaskStatus.ERROR_RECOVERABLE, TaskStatus.ERROR_FATAL),
    ]

    def reset(
        self, name: str, command: str, max_attempts: int, reset_if_running: bool
    ) -> None:
        """Reset status and number of attempts on a Task."""
        # only reset undone tasks
        if self.status != TaskStatus.DONE:

            # only reset if the task is not currently running or if we are
            # resetting running tasks
            if self.status != TaskStatus.RUNNING or reset_if_running:
                self.status = TaskStatus.REGISTERED
                self.num_attempts = 0
                self.name = name
                self.command = command
                self.max_attempts = max_attempts
                self.status_date = func.now()

    def transition(self, new_state: str) -> None:
        """Transition the Task to a new state."""
        bind_to_logger(workflow_id=self.workflow_id, task_id=self.id)
        logger.info(f"Transitioning task from {self.status} to {new_state}")
        self._validate_transition(new_state)
        if new_state == TaskStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state
        self.status_date = func.now()

    def transition_after_task_instance_error(
        self, job_instance_error_state: str
    ) -> None:
        """Transition the task to an error state."""
        bind_to_logger(workflow_id=self.workflow_id, task_id=self.id)
        logger.info("Transitioning task to ERROR_RECOVERABLE")
        self.transition(TaskStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            logger.info("Giving up task after max attempts.")
            self.transition(TaskStatus.ERROR_FATAL)
        else:
            if job_instance_error_state == TaskInstanceStatus.RESOURCE_ERROR:
                logger.debug("Adjust resource for task.")
                self.transition(TaskStatus.ADJUSTING_RESOURCES)
            else:
                logger.debug("Retrying Task.")
                self.transition(TaskStatus.QUEUED_FOR_INSTANTIATION)

    def _validate_transition(self, new_state: str) -> None:
        """Ensure the task state transition is valid."""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition("Task", self.id, self.status, new_state)
