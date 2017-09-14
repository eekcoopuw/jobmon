from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes
from jobmon.models import Job, JobStatus
from jobmon.reply_server import ReplyServer


class JobQueryServer(ReplyServer):
    """This services basic queries surrounding jobs"""

    def __init__(self, rep_port=None):
        super().__init__(rep_port)
        self.register_action("get_active", self.get_instantiated_or_running)
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
            instantiated_jobs = session.query(Job).filter_by(
                status=JobStatus.INSTANTIATED,
                dag_id=dag_id).all()
            running_jobs = session.query(Job).filter_by(
                status=JobStatus.INSTANTIATED,
                dag_id=dag_id).all()
            jobs = instantiated_jobs + running_jobs
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)

    def get_jobs(self, dag_id):
        with session_scope() as session:
            jobs = session.query(Job).filter(Job.dag_id == dag_id).all()
            job_dcts = [j.to_wire() for j in jobs]
        return (ReturnCodes.OK, job_dcts)
