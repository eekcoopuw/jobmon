from datetime import datetime
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import contains_eager

from jobmon.config import config
from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes, NoDatabase
from jobmon.models import Job, JobInstance, JobStatus, JobInstanceStatus
from jobmon.reply_server import ReplyServer
from jobmon.meta_models import TaskDagMeta
from jobmon.workflow.workflow import WorkflowDAO


class JobQueryServer(ReplyServer):
    """This services basic queries surrounding jobs"""

    def __init__(self, rep_port=None):
        super().__init__(rep_port)
        self.register_action("get_submitted_or_running",
                             self.get_submitted_or_running)
        self.register_action("get_timed_out", self.get_timed_out)
        self.register_action("get_all_jobs", self.get_jobs)
        self.register_action("get_queued_for_instantiation",
                             self.get_queued_for_instantiation)
        self.register_action("get_dag_ids_by_hash",
                             self.get_dag_ids_by_hash)
        self.register_action("get_workflows_by_inputs",
                             self.get_workflows_by_inputs)

    def get_queued_for_instantiation(self, dag_id):
        with session_scope() as session:
            jobs = session.query(Job).filter_by(
                status=JobStatus.QUEUED_FOR_INSTANTIATION,
                dag_id=dag_id).all()
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)

    def get_submitted_or_running(self, dag_id):
        with session_scope() as session:
            instances = session.query(JobInstance).\
                filter(
                    JobInstance.status.in_([
                        JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                        JobInstanceStatus.RUNNING])).\
                join(Job).\
                options(contains_eager(JobInstance.job)).\
                filter_by(dag_id=dag_id).all()
            instances = [i.to_wire() for i in instances]
        return (ReturnCodes.OK, instances)

    def get_jobs(self, dag_id):
        """
        Return a dictionary mapping job_id to a dict of the job's instance
        variables

        Args
            dag_id:
        """
        with session_scope() as session:
            jobs = session.query(Job).filter(Job.dag_id == dag_id).all()
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)

    def get_timed_out(self, dag_id):
        with session_scope() as session:
            running = session.query(JobInstance).\
                filter(
                    JobInstance.status.in_([
                        JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
                        JobInstanceStatus.RUNNING])).\
                join(Job).\
                options(contains_eager(JobInstance.job)).\
                filter(Job.dag_id == dag_id,
                       Job.max_runtime != None).all()  # noqa: E711
            now = datetime.utcnow()
            timed_out = [r.to_wire() for r in running
                         if (now - r.status_date).seconds > r.job.max_runtime]
        return (ReturnCodes.OK, timed_out)

    def get_dag_ids_by_hash(self, dag_hash):
        """
        Return a dictionary mapping job_id to a dict of the job's instance
        variables

        Args
            dag_id:
        """
        with session_scope() as session:
            dags = session.query(TaskDagMeta).filter(
                TaskDagMeta.dag_hash == dag_hash).all()
            dag_ids = [dag.dag_id for dag in dags]
        return (ReturnCodes.OK, dag_ids)

    def get_workflows_by_inputs(self, dag_id, workflow_args):
        """
        Return a dictionary mapping job_id to a dict of the job's instance
        variables

        Args
            dag_id:
        """
        with session_scope() as session:
            workflow = session.query(WorkflowDAO).\
                filter(WorkflowDAO.dag_id.in_(dag_id)).\
                filter(WorkflowDAO.workflow_args == workflow_args).first()
            wf_id = workflow.id
        return (ReturnCodes.OK, wf_id)

    def listen(self):
        """If the database is unavailable, don't allow the JobQueryServer to
        start listening. This would defeat its purpose, as it wouldn't have
        anywhere to persist Job state..."""
        with session_scope() as session:
            try:
                session.connection()
            except OperationalError:
                raise NoDatabase("JobQueryServer could not connect to {}".
                                 format(config.conn_str))
        super().listen()
