"""Workflow Database Table."""
from jobmon.server.web.models import DB
from jobmon.server.web.models.exceptions import InvalidStateTransition
from jobmon.server.web.models.workflow_run import WorkflowRun
from jobmon.server.web.models.workflow_run_status import WorkflowRunStatus
from jobmon.server.web.models.workflow_status import WorkflowStatus

from sqlalchemy.sql import func


class Workflow(DB.Model):
    """Workflow Database Table."""

    __tablename__ = 'workflow'

    id = DB.Column(DB.Integer, primary_key=True)
    tool_version_id = DB.Column(DB.Integer, DB.ForeignKey("tool_version.id"))
    dag_id = DB.Column(DB.Integer, DB.ForeignKey('dag.id'))
    workflow_args_hash = DB.Column(DB.Integer)
    task_hash = DB.Column(DB.Integer)
    description = DB.Column(DB.Text(collation='utf8_general_ci'))
    name = DB.Column(DB.String(150))
    workflow_args = DB.Column(DB.Text(collation='utf8_general_ci'))
    max_concurrently_running = DB.Column(DB.Integer)
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_status.id'),
                       default=WorkflowStatus.REGISTERING)
    created_date = DB.Column(DB.DateTime, default=func.now())
    status_date = DB.Column(DB.DateTime, default=func.now())

    dag = DB.relationship("Dag", back_populates="workflow", lazy=True)
    workflow_runs = DB.relationship("WorkflowRun", back_populates="workflow", lazy=True)

    valid_transitions = [
        # normal progression from registered to a workflow run has been fully bound
        (WorkflowStatus.REGISTERING, WorkflowStatus.QUEUED),

        # workflow encountered an error before a workflow run was created.
        (WorkflowStatus.REGISTERING, WorkflowStatus.ABORTED),

        # a workflow aborted during task creation. new workflow launched, found
        # existing workflow id and is creating a new workflow run
        (WorkflowStatus.ABORTED, WorkflowStatus.REGISTERING),

        # new workflow run created that resumes old failed workflow run
        (WorkflowStatus.FAILED, WorkflowStatus.REGISTERING),

        # new workflow run created that resumes old suspended workflow run
        (WorkflowStatus.HALTED, WorkflowStatus.REGISTERING),

        # Workflow was bound but didn't start running. eventually moved
        # to failed.
        (WorkflowStatus.QUEUED, WorkflowStatus.FAILED),

        # workflow run was bound then started running. normal happy path
        (WorkflowStatus.QUEUED, WorkflowStatus.RUNNING),

        # workflow run was running and then got moved to a resume state
        (WorkflowStatus.RUNNING, WorkflowStatus.HALTED),

        # workflow run was running and then completed successfully
        (WorkflowStatus.RUNNING, WorkflowStatus.DONE),

        # workflow run was running and then failed with an error
        (WorkflowStatus.RUNNING, WorkflowStatus.FAILED),
    ]

    untimely_transitions = [
        (WorkflowStatus.REGISTERING, WorkflowStatus.REGISTERING)
    ]

    def transition(self, new_state: str):
        """Transition the state of the workflow."""
        if self._is_timely_transition(new_state):
            self._validate_transition(new_state)
            self.status = new_state
            self.status_date = func.now()

    def _validate_transition(self, new_state: str):
        """Ensure the Job state transition is valid."""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition('Workflow', self.id, self.status, new_state)

    def _is_timely_transition(self, new_state):
        """Check if the transition is invalid due to a race condition."""
        if (self.status, new_state) in self.untimely_transitions:
            return False
        else:
            return True

    def link_workflow_run(self, workflow_run: WorkflowRun, next_report_increment: float):
        """Link a workflow run to this workflow."""
        linked_wfr = [wfr.status == WorkflowRunStatus.LINKING for wfr in self.workflow_runs]
        if not any(linked_wfr) and self.ready_to_link:
            workflow_run.heartbeat(next_report_increment, WorkflowRunStatus.LINKING)
            current_wfr = [(workflow_run.id, workflow_run.status)]
        # active workflow run, don't bind.
        elif not any(linked_wfr) and not self.ready_to_link:
            # return currently alive workflow instead
            current_wfr = [(wfr.id, wfr.status) for wfr in self.workflow_runs if wfr.is_alive]
        # currently linked workflow run
        else:
            current_wfr = [(wfr.id, wfr.status) for wfr in self.workflow_runs]
        return current_wfr[0]

    def resume(self, reset_running_jobs: bool) -> None:
        """Resume a workflow."""
        for workflow_run in self.workflow_runs:
            if workflow_run.is_active:
                if reset_running_jobs:
                    workflow_run.cold_reset()
                else:
                    workflow_run.hot_reset()

    @property
    def ready_to_link(self):
        """Is this workflow able to link a new workflow run."""
        return self.status not in [WorkflowStatus.QUEUED, WorkflowStatus.RUNNING,
                                   WorkflowStatus.DONE]

    @property
    def is_resumable(self):
        """Is this workflow resumable."""
        return not any([wfr.is_alive for wfr in self.workflow_runs])
