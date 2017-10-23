import logging

import zmq

from jobmon import models
from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes
from jobmon.pubsub_helpers import mogrify
from jobmon.reply_server import ReplyServer
from jobmon.workflow import job_dag

# logging does not work well in python < 2.7 with Threads,
# see https://docs.python.org/2/library/logging.html
# Logging has to be set up BEFORE the Thread
# Therefore see tests/conf_test.py
logger = logging.getLogger(__name__)

class JobStateManager(ReplyServer):

    def __init__(self, rep_port=None, pub_port=None):
        super().__init__(rep_port)
        self.register_action("add_job", self.add_job)
        self.register_action("add_job_dag", self.add_job_dag)
        self.register_action("add_job_instance", self.add_job_instance)
        self.register_action("log_done", self.log_done)
        self.register_action("log_error", self.log_error)
        self.register_action("log_executor_id", self.log_executor_id)
        self.register_action("log_running", self.log_running)
        self.register_action("log_usage", self.log_usage)
        self.register_action("queue_job", self.queue_job)

        ctx = zmq.Context.instance()
        self.publisher = ctx.socket(zmq.PUB)
        self.publisher.setsockopt(zmq.LINGER, 0)
        if pub_port:
            self.pub_port = pub_port
            self.publisher.bind('tcp://*:{}'.format(self.pub_port))
        else:
            self.pub_port = self.publisher.bind_to_random_port('tcp://*')
        logger.info("Publishing to port {}".format(self.pub_port))

    def add_job(self, name, command, dag_id, slots=1, mem_free=2, project=None,
                max_attempts=1, max_runtime=None, context_args="{}"):
        job = models.Job(
            name=name,
            command=command,
            dag_id=dag_id,
            slots=slots,
            mem_free=mem_free,
            project=project,
            max_attempts=max_attempts,
            max_runtime=max_runtime,
            context_args=context_args,
            status=models.JobStatus.REGISTERED)
        with session_scope() as session:
            session.add(job)
            session.commit()
            job_id = job.job_id
        return (ReturnCodes.OK, job_id)

    def add_job_dag(self, name, user):
        dag = job_dag.JobDag(
            name=name,
            user=user)
        with session_scope() as session:
            session.add(dag)
            session.commit()
            dag_id = dag.dag_id
        return (ReturnCodes.OK, dag_id)

    def add_job_instance(self, job_id, executor_type):
        logger.debug("Add JI for job {}".format(job_id))
        job_instance = models.JobInstance(
            executor_type=executor_type,
            job_id=job_id)
        with session_scope() as session:
            session.add(job_instance)
            session.commit()
            ji_id = job_instance.job_instance_id

            # TODO: Would prefer putting this in the model, but can't find the
            # right post-create hook. Investigate.
            job_instance.job.transition(models.JobStatus.INSTANTIATED)
        return (ReturnCodes.OK, ji_id)

    def stop_listening(self):
        super().stop_listening()
        self.publisher.close()

    def log_done(self, job_instance_id):
        logger.debug("Log DONE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance_state(session, ji,
                                            models.JobInstanceStatus.DONE)
        return (ReturnCodes.OK,)

    def log_error(self, job_instance_id, error_message):
        logger.debug("Log ERROR for JI {}, message={}".format(job_instance_id, error_message))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance_state(session, ji,
                                            models.JobInstanceStatus.ERROR)
            error = models.JobInstanceErrorLog(job_instance_id=job_instance_id,
                                               description=error_message)
            session.add(error)
        return (ReturnCodes.OK,)

    def log_executor_id(self, job_instance_id, executor_id):
        logger.debug("Log EXECUTOR_ID for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance_state(
                session, ji,
                models.JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
            self._update_job_instance(session, ji, executor_id=executor_id)
        return (ReturnCodes.OK,)

    def log_running(self, job_instance_id):
        logger.debug("Log RUNNING for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance_state(session, ji,
                                            models.JobInstanceStatus.RUNNING)
        return (ReturnCodes.OK,)

    def log_usage(self, job_instance_id, usage_str=None, wallclock=None,
                  maxvmem=None, cpu=None, io=None):
        logger.debug("Log USAGE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance(session, ji, usage_str=usage_str,
                                      wallclock=wallclock, maxvmem=maxvmem,
                                      cpu=cpu, io=io)
        return (ReturnCodes.OK,)

    def queue_job(self, job_id):
        logger.debug("Queue Job {}".format(job_id))
        with session_scope() as session:
            job = session.query(models.Job).filter_by(job_id=job_id).first()
            job.transition(models.JobStatus.QUEUED_FOR_INSTANTIATION)
        return (ReturnCodes.OK,)

    def _get_job_instance(self, session, job_instance_id):
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        return job_instance

    def _update_job_instance_state(self, session, job_instance, status_id):
        logger.debug("Update JI state {} for  {}".format(status_id, job_instance))
        job_instance.transition(status_id)
        job = job_instance.job
        if job.status in [models.JobStatus.DONE, models.JobStatus.ERROR_FATAL]:
            msg = mogrify(job.dag_id, (job.job_id, job.status))
            self.publisher.send_string(msg)
        return (ReturnCodes.OK,)

    def _update_job_instance(self, session, job_instance, **kwargs):
        logger.debug("Update JI  {}".format(job_instance))
        for k, v in kwargs.items():
            setattr(job_instance, k, v)
        return (ReturnCodes.OK,)
