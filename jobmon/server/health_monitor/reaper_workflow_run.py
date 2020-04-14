from __future__ import annotations
from typing import Dict, List
from datetime import datetime

from jobmon.serializers import SerializeWorkflowRun


class ReaperWorkflowRun(object):
    def __init__(self, workflow_run_id: int, workflow_id: int, heartbeat_date: datetime):
        """
        Implementing workflow reaper behavior of workflow run

        Args
            workflow_run_id: id of workflow run object from db
            workflow_id: id of associated workflow
            heartbeat_date: the last time a workflow_run logged that it was alive in date format
        """
        self.workflow_run_id = workflow_run_id
        self.workflow_id = workflow_id
        self.heartbeat_date = heartbeat_date


    @staticmethod
    def from_wire(wire_tuple: tuple) -> ReaperWorkflowRun:
        kwargs = SerializeWorkflowRun.kwargs_from_wire(wire_tuple)
        return ReaperWorkflowRun(kwargs["id"], kwargs["workflow_id"], kwargs["heartbeat_date"])

    def to_wire(self) -> tuple:
        return SerializeWorkflowRun.to_wire(self.workflow_run_id, self.workflow_id, self.heartbeat_date)


class ReaperWorkflowRunResponse(object):
    """This class implements a list of ReaperWorkflowRuns"""
    workflow_runs: List[ReaperWorkflowRun]

    def __init__(self, workflow_runs: List[ReaperWorkflowRun]):
        self.workflow_runs = workflow_runs

    @staticmethod
    def from_wire(wire: Dict) -> ReaperWorkflowRunResponse:
        wfr_list = []
        for wfr in wire["workflow_runs"]:
            wfr_list.append(ReaperWorkflowRun.from_wire(wfr))
        return ReaperWorkflowRunResponse(wfr_list)

    def to_wire(self) -> Dict[str, List]:
        wfr_list = []
        for wfr in self.workflow_runs:
            wfr_list.append(wfr.to_wire())
        return {"workflow_runs": wfr_list}
