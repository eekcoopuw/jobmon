import logging

from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.serializers import SerializeWorkflowRun

logger = logging.getLogger(__name__)


class WorkflowRun(DB.Model):

    __tablename__ = 'workflow_run'

    def to_wire_as_reaper_workflow_run(self) -> tuple:
        serialized = SerializeWorkflowRun.to_wire(
            id=self.id,
            workflow_id=self.workflow_id,
            heartbeat_date=self.heartbeat_date)
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    user = DB.Column(DB.String(150))
    executor_class = DB.Column(DB.String(150))
    jobmon_version = DB.Column(DB.String(150), default='UNKNOWN')
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_run_status.id'),
                       default=WorkflowRunStatus.RUNNING)

    created_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())
    heartbeat_date = DB.Column(DB.DateTime, default=func.now())

    workflow = DB.relationship("Workflow", back_populates="workflow_runs",
                               lazy=True)

    valid_transitions = [
        # a workflow run is created normally. All tasks are updated in the db
        # and the workflow run can move to bound state
        (WorkflowRunStatus.REGISTERED, WorkflowRunStatus.BOUND),

        # a workflow run is created normally. Something goes wrong while the
        # tasks are binding and the workflow run moves to error state
        (WorkflowRunStatus.REGISTERED, WorkflowRunStatus.ABORTED),

        # a workflow run is bound and then logs running
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.RUNNING),

        # a workflow run is bound and then an error occurs before it starts
        # running
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.ERROR),

        # a workflow run is bound and then a new workflow run is created
        # before the old workflow run moves into running state
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.COLD_RESUME),
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.HOT_RESUME),

        # the workflow starts running normally and finishes successfully
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.DONE),

        # the workflow starts running normally and the user stops it via a
        # keyboard interrupt
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.STOPPED),

        # the workflow is running and then a new workflow run is created
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.COLD_RESUME),
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.HOT_RESUME),

        # the workflow is running and then it's tasks hit errors
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.ERROR),

        # the workflow is set to resume and then it successfully shuts down
        (WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.TERMINATED),
        (WorkflowRunStatus.HOT_RESUME, WorkflowRunStatus.TERMINATED)
    ]

    untimely_transitions = [(WorkflowRunStatus.RUNNING, WorkflowRunStatus.RUNNING)]

    bound_error_states = [WorkflowRunStatus.STOPPED, WorkflowRunStatus.ERROR]

    def is_active(self):
        return self.status in [WorkflowRunStatus.BOUND,
                               WorkflowRunStatus.RUNNING]

    def heartbeat(self, next_report_increment):
        self.transition(WorkflowRunStatus.RUNNING)
        self.heartbeat_date = func.ADDTIME(
            func.now(), func.SEC_TO_TIME(next_report_increment))

    def transition(self, new_state):
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            old_state = self.status
            self.status = new_state
            self.status_date = func.now()
            if new_state == WorkflowRunStatus.BOUND:
                self.workflow.transition(WorkflowStatus.BOUND)
            elif new_state == WorkflowRunStatus.ABORTED:
                self.workflow.transition(WorkflowStatus.ABORTED)
            elif new_state == WorkflowRunStatus.RUNNING:
                self.workflow.transition(WorkflowStatus.RUNNING)
            elif new_state == WorkflowRunStatus.DONE:
                self.workflow.transition(WorkflowStatus.DONE)
            elif new_state == WorkflowRunStatus.COLD_RESUME:
                self.workflow.transition(WorkflowStatus.SUSPENDED)
            elif new_state == WorkflowRunStatus.HOT_RESUME:
                self.workflow.transition(WorkflowStatus.SUSPENDED)
            elif new_state == WorkflowRunStatus.TERMINATED:
                # if hot resume move to registered on term
                if old_state == WorkflowRunStatus.COLD_RESUME:
                    self.workflow.transition(WorkflowStatus.FAILED)
                elif old_state == WorkflowRunStatus.HOT_RESUME:
                    self.workflow.transition(WorkflowStatus.REGISTERED)

            elif new_state in self.bound_error_states:
                self.workflow.transition(WorkflowStatus.FAILED)

    def hot_reset(self):
        self.transition(WorkflowRunStatus.HOT_RESUME)

    def cold_reset(self):
        self.transition(WorkflowRunStatus.COLD_RESUME)

    def _validate_transition(self, new_state):
        """Ensure the Job state transition is valid"""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition('WorkflowRun', self.id, self.status,
                                         new_state)

    def _is_timely_transition(self, new_state):
        """Check if the transition is invalid due to a race condition"""

        if (self.status, new_state) in self.untimely_transitions:
            return False
        else:
            return True
