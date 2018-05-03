import logging
from datetime import datetime, timedelta
from time import sleep

from jobmon.config import config
from jobmon.database import session_scope
from jobmon.meta_models import TaskDagMeta
from jobmon.requester import Requester
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


logger = logging.getLogger(__name__)


class HealthMonitor(object):
    """Watches for disappearing dags / workflows

    Args:
        loss_threshold (int) (in minutes): consider a workflow run lost
            if the time since its last heartbeat exceeds this threshold
        poll_interval (int) (in minutes): time elapsed between successive
            checks for workflow run heartbeats. should be greater than
             the loss_threshold
        notification_sink (callable(str), optional): a callable that
            takes a string, where info can be sent whenever a lost
            workflow run is identified
    """

    def __init__(self, loss_threshold=5, poll_interval=10,
                 notification_sink=None):

        if poll_interval < loss_threshold:
            raise ValueError("poll_interval ({pi} min) must exceed the "
                             "loss_threshold ({lt} min)".format(
                                 pi=poll_interval,
                                 lt=loss_threshold))

        self._requester = Requester(config.jm_rep_conn)
        self._loss_threshold = timedelta(minutes=loss_threshold)
        self._poll_interval = poll_interval
        self._notification_sink = notification_sink

    def monitor_forever(self):
        while True:
            with session_scope() as session:
                lost_wrs = self._get_lost_workflow_runs(session)
                self._register_lost_workflow_runs(lost_wrs)
            sleep(self._poll_interval*60)

    def _get_active_workflow_runs(self, session):
        wrs = session.query(WorkflowRunDAO).filter_by(
            status=WorkflowRunStatus.RUNNING).all()
        return wrs

    def _get_lost_workflow_runs(self, session):
        wrs = self._get_active_workflow_runs(session)
        return [wr for wr in wrs if self._has_lost_workflow_run(wr)]

    def _has_lost_workflow_run(self, workflow_run):
        td = workflow_run.workflow.task_dag
        time_since_last_heartbeat = (datetime.utcnow() - td.heartbeat_date)
        return time_since_last_heartbeat > self._loss_threshold

    def _register_lost_workflow_runs(self, lost_workflow_runs):
        for wfr in lost_workflow_runs:
            self._requester.send_request({
                'action': 'update_workflow_run',
                'kwargs': {'wfr_id': wfr.id,
                           'status': WorkflowRunStatus.ERROR}
            })
            wf = wfr.workflow
            dag = wf.task_dag
            msg = ("Lost contact with Workflow Run #{wfr_id}:\n"
                   "    running on host: {hostname}\n"
                   "    PID: {pid}\n"
                   "    workflow_id: {wf_id}\n"
                   "    workflow_args: {wf_args}\n"
                   "    task_dag id: {dag_id}\n"
                   "    task_dag name: {dag_name}".format(
                       wfr_id=wfr.id, hostname=wfr.hostname, pid=wfr.pid,
                       wf_id=wf.id, wf_args=wf.workflow_args,
                       dag_id=dag.dag_id, dag_name=dag.name))
            logger.info(msg)
            if self._notification_sink:
                self._notification_sink(msg, channel=wfr.slack_channel)
