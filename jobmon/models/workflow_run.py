from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow_status import WorkflowStatus


class WorkflowRun(DB.Model):

    __tablename__ = 'workflow_run'

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    user = DB.Column(DB.String(150))
    executor_class = DB.Column(DB.String(150))
    jobmon_version = DB.Column(DB.String(150), default='UNKNOWN')
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_run_status.id'),
                       default=WorkflowRunStatus.RUNNING)

    created_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    heartbeat_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())

    workflow = DB.relationship("Workflow", backref="workflow_runs", lazy=True)

    valid_transitions = [
        # a workflow run is created normally. All tasks are updated in the db
        # and the workflow run can move to bound state
        (WorkflowRunStatus.CREATED, WorkflowRunStatus.BOUND),

        # a workflow run is created normally. Something goes wrong while the
        # tasks are binding and the workflow run moves to error state
        (WorkflowRunStatus.CREATED, WorkflowRunStatus.ABORTED),

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
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.ERROR)
        ]

    bound_error_states = [WorkflowRunStatus.STOPPED,
                          WorkflowRunStatus.COLD_RESUME,
                          WorkflowRunStatus.HOT_RESUME,
                          WorkflowRunStatus.ERROR]

    def transition(self, new_state):
        self._validate_transition(new_state)
        self.status = new_state
        self.status_date = func.UTC_TIMESTAMP()
        if new_state == WorkflowRunStatus.BOUND:
            self.workflow.transition(WorkflowStatus.BOUND)
        elif new_state == WorkflowRunStatus.ABORTED:
            self.workflow.transition(WorkflowStatus.ABORTED)
        elif new_state == WorkflowRunStatus.RUNNING:
            self.workflow.transition(WorkflowStatus.RUNNING)
        elif new_state == WorkflowRunStatus.DONE:
            self.workflow.transition(WorkflowStatus.DONE)
        elif new_state in self.bound_error_states:
            self.workflow.transition(WorkflowStatus.FAILED)

    def hot_reset(self):
        pass

    def cold_reset(self):
        pass

    def _validate_transition(self, new_state):
        """Ensure the Job state transition is valid"""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition('Workflow', self.id, self.status,
                                         new_state)
