import logging
from datetime import datetime, timedelta
from time import sleep

from jobmon.config import config
from jobmon.database import session_scope, engine
from jobmon.requester import Requester
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


logger = logging.getLogger(__name__)


class HealthMonitor(object):
    """Watches for disappearing dags / workflows, as well as failing nodes

    Args:
        loss_threshold (int) (in minutes): consider a workflow run lost
            if the time since its last heartbeat exceeds this threshold
        poll_interval (int) (in minutes): time elapsed between successive
            checks for workflow run heartbeats + failing nodes. should be
             greater than the loss_threshold
        wf_notification_sink (callable(str), optional): a callable that
            takes a string, where info can be sent whenever a lost
            workflow run is identified
        node_notification_sink (callable(str), optional): a callable that
            takes a string, where info can be sent whenever nodes have been
            found to be failing
    """

    def __init__(self, loss_threshold=5, poll_interval=10,
                 wf_notification_sink=None, node_notification_sink=None):

        if poll_interval < loss_threshold:
            raise ValueError("poll_interval ({pi} min) must exceed the "
                             "loss_threshold ({lt} min)".format(
                                 pi=poll_interval,
                                 lt=loss_threshold))

        self._requester = Requester(config.jm_rep_conn)
        self._loss_threshold = timedelta(minutes=loss_threshold)
        self._poll_interval = poll_interval
        self._wf_notification_sink = wf_notification_sink
        self._node_notification_sink = node_notification_sink
        self._database = engine.url.translate_connect_args()['database']

    def monitor_forever(self):
        while True:
            with session_scope() as session:
                lost_wrs = self._get_lost_workflow_runs(session)
                working_wf_runs = self._get_succeeding_active_workflow_runs(
                    session)
                failing_nodes = self._calculate_node_failure_rate(
                    session, working_wf_runs)
                self._register_lost_workflow_runs(lost_wrs)
                self._notify_of_failing_nodes(failing_nodes)
            sleep(self._poll_interval * 60)

    def _get_succeeding_active_workflow_runs(self, session):
        """Collect all active workflow runs that have < 10% failure rate"""
        query = (
            """SELECT
                workflow_run_id,
                (COUNT(CASE
                    WHEN ji.status = 'E' THEN job_instance_id
                    ELSE NULL
                END) / COUNT(job_instance_id)) AS failure_rate
            FROM
                {db}.job_instance ji
            JOIN
                {db}.workflow_run wf ON ji.workflow_run_id = wf.id
            WHERE
                wf.status = "{s}"
            GROUP BY workflow_run_id
            HAVING failure_rate <= .1
                 """.format(db=self._database, s=WorkflowRunStatus.RUNNING))
        res = session.execute(query).fetchall()
        if res:
            return [tup[0] for tup in res]
        return []

    def _calculate_node_failure_rate(self, session, working_wf_runs):
        """Collect all nodenames used in currently running,
        currently successful workflow runs, and report the ones that have at
        least 5 job instances and at least 50% failure rate on that node"""
        if not working_wf_runs:
            # no active/successful workflow runs have < 10% failure
            return []
        working_wf_runs = ", ".join(str(n) for n in working_wf_runs)
        query = (
            """SELECT
                nodename,
           (COUNT(CASE when ji.status = 'E' then job_instance_id else NULL END)
                / COUNT(job_instance_id)) as failure_rate
            FROM
                {db}.job_instance ji
            JOIN
                {db}.workflow_run wf ON ji.workflow_run_id = wf.id
            WHERE
                ji.workflow_run_id IN({wf})
            GROUP BY nodename
            HAVING COUNT(job_instance_id) > 5 and failure_rate > .2;"""
            .format(db=self._database, wf=working_wf_runs))
        res = session.execute(query).fetchall()
        if res:
            return [tup[0] for tup in res]
        return []

    def _notify_of_failing_nodes(self, nodes):
        if not nodes:
            return
        msg = "Potentially failing nodes found: {}".format(nodes)
        if self._node_notification_sink:
            self._node_notification_sink(msg)

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
            if self._wf_notification_sink:
                self._wf_notification_sink(msg=msg,
                                           channel=wfr.slack_channel)
