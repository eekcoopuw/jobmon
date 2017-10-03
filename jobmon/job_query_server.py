from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes
from jobmon.models import Job, JobInstance, JobStatus, JobInstanceStatus
from jobmon.reply_server import ReplyServer


class JobQueryServer(ReplyServer):
    """This services basic queries surrounding jobs"""

    def __init__(self, rep_port=None):
        super().__init__(rep_port)
        self.register_action("get_active", self.get_instantiated_or_running)
        self.register_action("get_active_executor_ids",
                             self.get_instantiated_or_running_executor_ids)
        self.register_action("get_all_jobs", self.get_jobs)
        self.register_action("get_queued_for_instantiation",
                             self.get_queued_for_instantiation)

    def get_queued_for_instantiation(self, dag_id):
        with session_scope() as session:
            jobs = session.query(Job).filter_by(
                status=JobStatus.QUEUED_FOR_INSTANTIATION,
                dag_id=dag_id).all()
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)

    def get_instantiated_or_running(self, dag_id):
        with session_scope() as session:
            instantiated_jobs = session.query(Job).\
                filter_by(status=JobStatus.INSTANTIATED, dag_id=dag_id).all()
            running_jobs = session.query(Job).\
                filter_by(status=JobStatus.RUNNING, dag_id=dag_id).all()
            jobs = instantiated_jobs + running_jobs
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)

    def get_instantiated_or_running_executor_ids(self, dag_id):
        with session_scope() as session:
            instantiated = session.query(JobInstance).\
                filter_by(
                    status=JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR).\
                join(Job).\
                filter_by(dag_id=dag_id).all()
            running = session.query(JobInstance).\
                filter_by(
                    status=JobInstanceStatus.RUNNING).\
                join(Job).\
                filter_by(dag_id=dag_id).all()
            job_instances = instantiated + running
            executor_ids = {j.job_instance_id: j.executor_id
                            for j in job_instances
                            if j.executor_id is not None}
        return (ReturnCodes.OK, executor_ids)

    def get_jobs(self, dag_id):
        with session_scope() as session:
            jobs = session.query(Job).filter(Job.dag_id == dag_id).all()
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)
