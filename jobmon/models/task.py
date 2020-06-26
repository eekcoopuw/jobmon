import logging

from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.models.task_status import TaskStatus
from jobmon.serializers import SerializeExecutorTask, SerializeSwarmTask


logger = logging.getLogger(__name__)


class Task(DB.Model):

    __tablename__ = "task"

    def to_wire_as_executor_task(self):
        lnode, lpgid = self._last_instance_procinfo()
        serialized = SerializeExecutorTask.to_wire(
            task_id=self.id,
            workflow_id=self.workflow_id,
            node_id=self.node_id,
            task_args_hash=self.task_args_hash,
            name=self.name,
            command=self.command,
            status=self.status,
            max_runtime_seconds=self.executor_parameter_set.max_runtime_seconds,
            context_args=self.executor_parameter_set.context_args,
            resource_scales=self.executor_parameter_set.resource_scales,
            queue=self.executor_parameter_set.queue,
            num_cores=self.executor_parameter_set.num_cores,
            m_mem_free=self.executor_parameter_set.m_mem_free,
            j_resource=self.executor_parameter_set.j_resource,
            hard_limits=self.executor_parameter_set.hard_limits)
        return serialized

    def to_wire_as_swarm_task(self):
        serialized = SerializeSwarmTask.to_wire(
            task_id=self.id,
            status=self.status)
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    node_id = DB.Column(DB.Integer, DB.ForeignKey('node.id'))
    task_args_hash = DB.Column(DB.Integer)
    name = DB.Column(DB.String(255))
    command = DB.Column(DB.Text)
    executor_parameter_set_id = DB.Column(
        DB.Integer, DB.ForeignKey('executor_parameter_set.id'),
        default=None)
    num_attempts = DB.Column(DB.Integer, default=0)
    max_attempts = DB.Column(DB.Integer, default=1)
    status = DB.Column(DB.String(1), DB.ForeignKey('task_status.id'))
    submitted_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())

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

    def reset(self, name, command, max_attempts, reset_if_running):
        """Reset status and number of attempts on a Task"""
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

    def transition(self, new_state):
        """Transition the Task to a new state"""
        self._validate_transition(new_state)
        if new_state == TaskStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state
        self.status_date = func.now()
        logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logger.info("transite job status to {s} at {t}".format(s=new_state, t=self.status_date))

    def transition_after_task_instance_error(self, job_instance_error_state):
        """Transition the Job to an error state"""
        self.transition(TaskStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            logger.debug("ZZZ GIVING UP Job {}".format(self.id))
            self.transition(TaskStatus.ERROR_FATAL)
        else:
            if job_instance_error_state == TaskInstanceStatus.RESOURCE_ERROR:
                self.transition(TaskStatus.ADJUSTING_RESOURCES)
            else:
                logger.debug("ZZZ retrying Job {}".format(self.id))
                self.transition(TaskStatus.QUEUED_FOR_INSTANTIATION)

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
