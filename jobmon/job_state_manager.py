from jobmon import models
from jobmon.database import Session
from jobmon.exceptions import ReturnCodes
from jobmon.reply_server import ReplyServer


class JobStateManager(ReplyServer):

    def __init__(self, port=None):
        super().__init__(port)
        self.register_action("add_job", self.add_job)
        self.register_action("add_job_dag", self.add_job_dag)
        self.register_action("add_job_instance", self.add_job_instance)
        self.register_action("log_done", self.log_done)
        self.register_action("log_error", self.log_error)
        self.register_action("log_running", self.log_running)
        self.register_action("log_usage", self.log_usage)
        self.register_action("queue_job", self.queue_job)

    def add_job_dag(self, name, user):
        dag = models.JobDag(
            name=name,
            user=user)
        session = Session()
        session.add(dag)
        session.commit()
        dag_id = dag.dag_id
        session.close()
        return ReturnCodes.OK, dag_id

    def add_job(self, name, runfile, job_args, dag_id, max_attempts=1):
        job = models.Job(
            name=name,
            runfile=runfile,
            args=job_args,
            dag_id=dag_id,
            max_attempts=max_attempts,
            status=models.JobStatus.REGISTERED)
        session = Session()
        session.add(job)
        session.commit()
        job_id = job.job_id
        session.close()
        return ReturnCodes.OK, job_id

    def add_job_instance(self, job_id, job_instance_id):
        job_instance = models.JobInstance(
            job_instance_id=job_instance_id,
            job_instance_type='SGE',
            job_id=job_id)
        session = Session()
        session.add(job_instance)
        session.commit()
        ji_id = job_instance.job_instance_id

        # TODO: Would prefer putting this in the model, but can't find the
        # right post-create hook. Investigate.
        job_instance.job.transition(models.JobStatus.INSTANTIATED)
        session.commit()
        session.close()
        return ReturnCodes.OK, ji_id

    def log_error(self, job_instance_id, error_message):
        self._update_job_instance_state(job_instance_id,
                                        models.JobInstanceStatus.ERROR)
        session = Session()
        error = models.JobInstanceErrorLog(job_instance_id=job_instance_id,
                                           description=error_message)
        session.add(error)
        session.commit()
        session.close()
        return (ReturnCodes.OK, job_instance_id)

    def log_done(self, job_instance_id):
        return self._update_job_instance_state(job_instance_id,
                                               models.JobInstanceStatus.DONE)

    def log_running(self, job_instance_id):
        return self._update_job_instance_state(
            job_instance_id, models.JobInstanceStatus.RUNNING)

    def log_usage(self, job_instance_id, **kwargs):
        session = Session()
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        for k, v in kwargs.items():
            setattr(job_instance, k, v)
        session.add(job_instance)
        session.commit()
        session.close()
        return (ReturnCodes.OK,)

    def queue_job(self, job_id):
        session = Session()
        job = session.query(models.Job).filter_by(job_id=job_id).first()
        job.transition(models.JobStatus.QUEUED_FOR_INSTANTIATION)
        session.commit()
        session.close()
        return (ReturnCodes.OK,)

    def _update_job_instance_state(self, job_instance_id, status_id):
        session = Session()
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        job_instance.transition(status_id)
        session.commit()
        session.close()
        return (ReturnCodes.OK,)
