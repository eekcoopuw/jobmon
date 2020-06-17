from datetime import datetime
from sqlalchemy.sql import func

from jobmon.models import DB
from jobmon.models.exceptions import InvalidStateTransition
from jobmon.models.workflow_status import WorkflowStatus


class Workflow(DB.Model):

    __tablename__ = 'workflow'

    @classmethod
    def from_wire(cls, dct):
        pass

    def to_wire(self):
        pass

    id = DB.Column(DB.Integer, primary_key=True)
    tool_version_id = DB.Column(DB.Integer, DB.ForeignKey("tool_version.id"))
    dag_id = DB.Column(DB.Integer, DB.ForeignKey('dag.id'))
    workflow_args_hash = DB.Column(DB.Integer)
    task_hash = DB.Column(DB.Integer)
    description = DB.Column(DB.Text(collation='utf8_general_ci'))
    name = DB.Column(DB.String(150))
    workflow_args = DB.Column(DB.Text(collation='utf8_general_ci'))
    status = DB.Column(DB.String(1),
                       DB.ForeignKey('workflow_status.id'),
                       default=WorkflowStatus.REGISTERED)
    heartbeat_date = DB.Column(DB.DateTime, default=datetime.utcnow)
    created_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())
    status_date = DB.Column(DB.DateTime, default=func.UTC_TIMESTAMP())

    dag = DB.relationship("Dag", back_populates="workflow", lazy=True)
    workflow_runs = DB.relationship("WorkflowRun", back_populates="workflow",
                                    lazy=True)

    valid_transitions = [
        # normal progression from registered to a workflow run has been created
        (WorkflowStatus.REGISTERED, WorkflowStatus.CREATED),

        # workflow encountered an error before a workflow run was created.
        (WorkflowStatus.REGISTERED, WorkflowStatus.ABORTED),

        # a workflow aborted during task creation. new workflow launched, found
        # existing workflow id and is creating a new workflow run
        (WorkflowStatus.ABORTED, WorkflowStatus.CREATED),

        # new workflow run created that resumes old failed workflow run
        (WorkflowStatus.FAILED, WorkflowStatus.CREATED),

        # new workflow run created that resumes old suspended workflow run
        (WorkflowStatus.SUSPENDED, WorkflowStatus.CREATED),

        # Workflow run has been created then all tasks are bound. normal
        # happy path
        (WorkflowStatus.CREATED, WorkflowStatus.BOUND),

        # Workflow run was created but workflow didn't add all tasks
        # successfully or otherwise errored out before workflow was usable
        (WorkflowStatus.CREATED, WorkflowStatus.ABORTED),

        # Workflow was bound but didn't start running. eventually moved
        # to failed.
        (WorkflowStatus.BOUND, WorkflowStatus.FAILED),

        # workflow run was bound then started running. normal happy path
        (WorkflowStatus.BOUND, WorkflowStatus.RUNNING),

        # workflow run was running and then got moved to a resume state
        (WorkflowStatus.RUNNING, WorkflowStatus.SUSPENDED),

        # workflow run was running and then completed successfully
        (WorkflowStatus.RUNNING, WorkflowStatus.DONE),

        # workflow run was running and then failed with an error
        (WorkflowStatus.RUNNING, WorkflowStatus.FAILED),

        # workflow run was set to a resume state and successfully shut down.
        # moved to failed
        (WorkflowStatus.SUSPENDED, WorkflowStatus.FAILED)
        ]

    def transition(self, new_state):
        self._validate_transition(new_state)
        self.status = new_state
        self.status_date = func.UTC_TIMESTAMP()

    def _validate_transition(self, new_state):
        """Ensure the Job state transition is valid"""
        if (self.status, new_state) not in self.valid_transitions:
            raise InvalidStateTransition('Workflow', self.id, self.status,
                                         new_state)

    def resume(self, reset_running_jobs):
        resumed_wfr = []
        for workflow_run in self.workflow_runs:
            if workflow_run.is_active():
                if reset_running_jobs:
                    workflow_run.cold_reset()
                else:
                    workflow_run.hot_reset()
                resumed_wfr.append(workflow_run)
        return resumed_wfr
