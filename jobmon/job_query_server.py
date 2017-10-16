from datetime import datetime

from sqlalchemy.orm import contains_eager

from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes
from jobmon.models import Job, JobInstance, JobStatus, JobInstanceStatus
from jobmon.reply_server import ReplyServer


class JobQueryServer(ReplyServer):
    """This services basic queries surrounding jobs"""

    @staticmethod
    def it():
        if not JobQueryServer.the_instance:
            JobQueryServer.the_instance = JobQueryServer()
        return JobQueryServer.the_instance

    def __init__(self, rep_port=None):
        super().__init__(rep_port)
        self.register_action("get_submitted_or_running",
                             self.get_submitted_or_running)
        self.register_action("get_timed_out", self.get_timed_out)
        self.register_action("get_all_jobs", self.get_jobs)
        self.register_action("get_queued_for_instantiation",
                             self.get_queued_for_instantiation)
        # And maintain singleton semantics, set the singleton instance variable
        JobQueryServer.the_instance = self

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
        Returna dictionary mapping job_id to a dict of the job's instance variables
        :param dag_id:
        :return:
        """
        with session_scope() as session:
            jobs = session.query(Job).filter(Job.dag_id == dag_id).all()
            job_dictionary = {}
            for j in jobs:
                job_dictionary[j.job_id] = j
        return (ReturnCodes.OK, job_dictionary)

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
                         if (now-r.status_date).seconds > r.job.max_runtime]
        return (ReturnCodes.OK, timed_out)
