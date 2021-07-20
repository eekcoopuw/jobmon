"""Workflow run database table."""
from flask import current_app as app
from jobmon.serializers import SerializeWorkflowRun
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.models.workflow_status import WorkflowStatus
from sqlalchemy.sql import func


class WorkflowRun(DB.Model):
    """Database table for recording Workflow Runs."""

    __tablename__ = 'workflow_run'

    def to_wire_as_reaper_workflow_run(self) -> tuple:
        """Serialize workflow run."""
        serialized = SerializeWorkflowRun.to_wire(
            id=self.id,
            workflow_id=self.workflow_id
        )
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    workflow_id = DB.Column(DB.Integer, DB.ForeignKey('workflow.id'))
    user = DB.Column(DB.String(150))
    jobmon_version = DB.Column(DB.String(150), default='UNKNOWN')
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_run_status.id'),
                       default=WorkflowRunStatus.RUNNING)

    created_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())
    heartbeat_date = DB.Column(DB.DateTime, default=func.now())

    workflow = DB.relationship("Workflow", back_populates="workflow_runs", lazy=True)

    valid_transitions = [
        # a workflow run is created normally. claimed control of workflow
        (WorkflowRunStatus.REGISTERED, WorkflowRunStatus.LINKING),

        # a workflow run is created normally. All tasks are updated in the db
        # and the workflow run can move to bound state
        (WorkflowRunStatus.LINKING, WorkflowRunStatus.BOUND),

        # a workflow run is created normally. Something goes wrong while the
        # tasks are binding and the workflow run moves to error state
        (WorkflowRunStatus.LINKING, WorkflowRunStatus.ABORTED),

        # a workflow run is bound and then moves to instantiating
        (WorkflowRunStatus.BOUND, WorkflowRunStatus.INSTANTIATING),

        # a workflow run moves from instantiating to launched
        (WorkflowRunStatus.INSTANTIATING, WorkflowRunStatus.LAUNCHED),

        # a workflow run moves from launched to running
        (WorkflowRunStatus.LAUNCHED, WorkflowRunStatus.RUNNING),

        # a workflow run can't be launched for some reason. TODO: implement triaging
        (WorkflowRunStatus.INSTANTIATING, WorkflowRunStatus.ERROR),
        (WorkflowRunStatus.LAUNCHED, WorkflowRunStatus.ERROR),

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

    untimely_transitions = [
        (WorkflowRunStatus.RUNNING, WorkflowRunStatus.RUNNING),
        (WorkflowRunStatus.LINKING, WorkflowRunStatus.LINKING)
    ]

    bound_error_states = [WorkflowRunStatus.STOPPED, WorkflowRunStatus.ERROR]

    active_states = [
        WorkflowRunStatus.BOUND, WorkflowRunStatus.RUNNING, WorkflowRunStatus.COLD_RESUME,
        WorkflowRunStatus.HOT_RESUME
    ]

    @property
    def is_alive(self) -> bool:
        """Workflow run is in a state that should be registering heartbeats."""
        return self.status in [WorkflowRunStatus.LINKING, WorkflowRunStatus.BOUND,
                               WorkflowRunStatus.RUNNING, WorkflowRunStatus.COLD_RESUME,
                               WorkflowRunStatus.HOT_RESUME]

    @property
    def is_active(self) -> bool:
        """Statuses where Workflow Run is active (bound or running)."""
        return self.status in [WorkflowRunStatus.BOUND, WorkflowRunStatus.RUNNING]

    def heartbeat(self, next_report_increment: float,
                  transition_status: str = WorkflowRunStatus.RUNNING) -> None:
        """Register a heartbeat for the Workflow Run to show it is still alive."""
        self.transition(transition_status)
        self.heartbeat_date = func.ADDTIME(func.now(), func.SEC_TO_TIME(next_report_increment))

    def reap(self) -> None:
        """Transition dead workflow runs to a terminal state."""
        app.logger = app.logger.bind(workflow_run_id=self.id,
                                     workflow_id=self.workflow_id)
        app.logger.info(f"Dead wfr {self.id} will be transitted.")
        if self.status == WorkflowRunStatus.LINKING:
            app.logger.debug(f"Transiting wfr {self.id} to ABORTED")
            self.transition(WorkflowRunStatus.ABORTED)
        if self.status in [WorkflowRunStatus.COLD_RESUME, WorkflowRunStatus.HOT_RESUME]:
            app.logger.debug(f"Transiting wfr {self.id} to TERMINATED")
            self.transition(WorkflowRunStatus.TERMINATED)
        if self.status == WorkflowRunStatus.RUNNING:
            app.logger.debug(f"Transiting wfr {self.id} to ERROR")
            self.transition(WorkflowRunStatus.ERROR)
        app.logger.info(f"Transited wfr {self.id} to {self.status}")

    def transition(self, new_state: str) -> None:
        """Transition the Workflow Run's state."""
        app.logger = app.logger.bind(workflow_run_id=self.id,
                                     workflow_id=self.workflow_id)
        app.logger.info(f"Transitting wfr {self.id} from {self.status} "
                        f"to {new_state}")
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            self.status = new_state
            self.status_date = func.now()
            if new_state == WorkflowRunStatus.LINKING:
                self.workflow.transition(WorkflowStatus.REGISTERING)
            elif new_state == WorkflowRunStatus.BOUND:
                self.workflow.transition(WorkflowStatus.QUEUED)
            elif new_state == WorkflowRunStatus.ABORTED:
                self.workflow.transition(WorkflowStatus.ABORTED)
            elif new_state == WorkflowRunStatus.RUNNING:
                self.workflow.transition(WorkflowStatus.RUNNING)
            elif new_state == WorkflowRunStatus.DONE:
                self.workflow.transition(WorkflowStatus.DONE)
            elif new_state == WorkflowRunStatus.TERMINATED:
                self.workflow.transition(WorkflowStatus.HALTED)
            elif new_state in self.bound_error_states:
                self.workflow.transition(WorkflowStatus.FAILED)
            elif new_state == WorkflowRunStatus.INSTANTIATING:
                self.workflow.transition(WorkflowStatus.INSTANTIATING)
            elif new_state == WorkflowRunStatus.LAUNCHED:
                self.workflow.transition(WorkflowStatus.LAUNCHED)

    def hot_reset(self) -> None:
        """Set Workflow Run to Hot Resume."""
        app.logger = app.logger.bind(workflow_run_id=self.id,
                                     workflow_id=self.workflow_id)
        app.logger.info(f"Transitting wfr {self.id} to HOT_RESUME.")
        self.transition(WorkflowRunStatus.HOT_RESUME)

    def cold_reset(self) -> None:
        """Set Workflow Run to Cold Resume."""
        app.logger = app.logger.bind(workflow_run_id=self.id,
                                     workflow_id=self.workflow_id)
        app.logger.info(f"Transitting wfr {self.id} to COLD_RESUME.")
        self.transition(WorkflowRunStatus.COLD_RESUME)

    def _validate_transition(self, new_state: str) -> None:
        """Ensure the Job state transition is valid."""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition('WorkflowRun', self.id, self.status, new_state)

    def _is_timely_transition(self, new_state: str) -> bool:
        """Check if the transition is invalid due to a race condition."""
        app.logger = app.logger.bind(workflow_run_id=self.id,
                                     workflow_id=self.workflow_id)
        if (self.status, new_state) in self.untimely_transitions:
            app.logger.info(f"Race condition when transitting wfr {self.id}")
            return False
        else:
            app.logger.debug(f"No race condition when transitting wfr {self.id}")
            return True
