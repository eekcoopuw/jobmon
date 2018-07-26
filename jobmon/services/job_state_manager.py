import logging
from datetime import datetime

import zmq
from sqlalchemy.exc import OperationalError

from jobmon import models
from jobmon.config import config
from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes, NoDatabase
from jobmon.pubsub_helpers import mogrify
from jobmon.reply_server import ReplyServer
from jobmon.meta_models import task_dag
from jobmon.workflow.workflow import WorkflowDAO
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus
from jobmon.attributes import attribute_models

# logging does not work well in python < 2.7 with Threads,
# see https://docs.python.org/2/library/logging.html
# Logging has to be set up BEFORE the Thread
# Therefore see tests/conf_test.py
logger = logging.getLogger(__name__)


class JobStateManager(ReplyServer):

    def __init__(self, rep_port=None, pub_port=None):
        super(JobStateManager, self).__init__(rep_port)
        self.register_action("add_job", self.add_job)
        self.register_action("add_task_dag", self.add_task_dag)
        self.register_action("add_job_instance", self.add_job_instance)
        self.register_action("add_workflow", self.add_workflow)
        self.register_action("add_workflow_run", self.add_workflow_run)
        self.register_action("update_workflow", self.update_workflow)
        self.register_action("update_workflow_run", self.update_workflow_run)
        self.register_action("is_workflow_running",
                             self.is_workflow_running)
        self.register_action("get_job_instances_of_workflow_run",
                             self.get_job_instances_of_workflow_run)

        self.register_action("log_done", self.log_done)
        self.register_action("log_error", self.log_error)
        self.register_action("log_executor_id", self.log_executor_id)
        self.register_action("log_heartbeat", self.log_heartbeat)
        self.register_action("log_running", self.log_running)
        self.register_action("log_nodename", self.log_nodename)
        self.register_action("log_usage", self.log_usage)

        self.register_action("queue_job", self.queue_job)
        self.register_action("reset_job", self.reset_job)
        self.register_action("reset_incomplete_jobs",
                             self.reset_incomplete_jobs)

        self.register_action("add_workflow_attribute",
                             self.add_workflow_attribute)
        self.register_action("add_workflow_run_attribute",
                             self.add_workflow_run_attribute)
        self.register_action("add_job_attribute",
                             self.add_job_attribute)

        ctx = zmq.Context.instance()
        self.publisher = ctx.socket(zmq.PUB)
        self.publisher.setsockopt(zmq.LINGER, 0)
        if pub_port:
            self.pub_port = pub_port
            self.publisher.bind('tcp://*:{}'.format(self.pub_port))
        else:
            self.pub_port = self.publisher.bind_to_random_port('tcp://*')
        logger.info("Publishing to port {}".format(self.pub_port))

    def add_job(self, name, job_hash, command, dag_id, slots=1,
                mem_free=2, max_attempts=1, max_runtime=None,
                context_args="{}", tag=None):
        job = models.Job(
            name=name,
            tag=tag,
            job_hash=job_hash,
            command=command,
            dag_id=dag_id,
            slots=slots,
            mem_free=mem_free,
            max_attempts=max_attempts,
            max_runtime=max_runtime,
            context_args=context_args,
            status=models.JobStatus.REGISTERED)
        with session_scope() as session:
            session.add(job)
            session.commit()
            job_dct = job.to_wire()
        return (ReturnCodes.OK, job_dct)

    def add_task_dag(self, name, user, dag_hash, created_date):
        dag = task_dag.TaskDagMeta(
            name=name,
            user=user,
            dag_hash=dag_hash,
            created_date=created_date)
        with session_scope() as session:
            session.add(dag)
            session.commit()
            dag_id = dag.dag_id
        return (ReturnCodes.OK, dag_id)

    def _get_workflow_run_id(self, job_id):
        with session_scope() as session:
            job = session.query(models.Job).filter_by(job_id=job_id).first()
            wf = session.query(WorkflowDAO).filter_by(dag_id=job.dag_id
                                                      ).first()
            if not wf:
                return None  # no workflow has started, so no workflow run
            wf_run = (session.query(WorkflowRunDAO).
                      filter_by(workflow_id=wf.id).
                      order_by(WorkflowRunDAO.id.desc()).first())
            wf_run_id = wf_run.id
        return wf_run_id

    def add_job_instance(self, job_id, executor_type):
        logger.debug("Add JI for job {}".format(job_id))
        workflow_run_id = self._get_workflow_run_id(job_id)
        job_instance = models.JobInstance(
            executor_type=executor_type,
            job_id=job_id,
            workflow_run_id=workflow_run_id)
        with session_scope() as session:
            session.add(job_instance)
            session.commit()
            ji_id = job_instance.job_instance_id

            # TODO: Would prefer putting this in the model, but can't find the
            # right post-create hook. Investigate.
            job_instance.job.transition(models.JobStatus.INSTANTIATED)
        return (ReturnCodes.OK, ji_id)

    def add_workflow(self, dag_id, workflow_args, workflow_hash, name, user,
                     description=""):
        wf = WorkflowDAO(dag_id=dag_id, workflow_args=workflow_args,
                         workflow_hash=workflow_hash, name=name, user=user,
                         description=description)
        with session_scope() as session:
            session.add(wf)
            session.commit()
            wf_dct = wf.to_wire()
        return (ReturnCodes.OK, wf_dct)

    def add_workflow_run(self, workflow_id, user, hostname, pid, stderr,
                         stdout, project, slack_channel, working_dir):
        wfr = WorkflowRunDAO(workflow_id=workflow_id,
                             user=user,
                             hostname=hostname,
                             pid=pid,
                             stderr=stderr,
                             stdout=stdout,
                             project=project,
                             slack_channel=slack_channel,
                             working_dir=working_dir)
        with session_scope() as session:
            workflow = session.query(WorkflowDAO).\
                filter(WorkflowDAO.id == workflow_id).first()
            # Set all previous runs to STOPPED
            for run in workflow.workflow_runs:
                run.status = WorkflowRunStatus.STOPPED
            session.add(wfr)
            session.commit()
            wfr_id = wfr.id
        return (ReturnCodes.OK, wfr_id)

    def update_workflow(self, wf_id, status):
        with session_scope() as session:
            wf = session.query(WorkflowDAO).\
                filter(WorkflowDAO.id == wf_id).first()
            wf.status = status
            wf.status_date = datetime.utcnow()
            session.commit()
            wf_dct = wf.to_wire()
        return (ReturnCodes.OK, wf_dct)

    def update_workflow_run(self, wfr_id, status):
        with session_scope() as session:
            wfr = session.query(WorkflowRunDAO).\
                filter(WorkflowRunDAO.id == wfr_id).first()
            wfr.status = status
            wfr.status_date = datetime.utcnow()
            session.commit()
        return (ReturnCodes.OK, status)

    def is_workflow_running(self, workflow_id):
        """Check if a previous workflow run for your user is still running """
        with session_scope() as session:
            wf_run = (session.query(WorkflowRunDAO).filter_by(
                workflow_id=workflow_id, status=WorkflowRunStatus.RUNNING,
            ).order_by(WorkflowRunDAO.id.desc()).first())
            if not wf_run:
                return (ReturnCodes.OK, False, {})
        return (ReturnCodes.OK, True, wf_run.to_wire())

    def get_job_instances_of_workflow_run(workflow_run_id):
        with session_scope() as session:
            jis = session.query(models.JobInstance).filter_by(
                workflow_run_id=workflow_run_id).all()
            jis = [ji.to_wire() for ji in jis]
        return (ReturnCodes.OK, jis)

    def listen(self):
        """If the database is unavailable, don't allow the JobStateManager to
        start listening. This would defeat its purpose, as it wouldn't have
        anywhere to persist Job state..."""
        with session_scope() as session:
            try:
                session.connection()
            except OperationalError:
                raise NoDatabase("JobStateManager could not connect to {}".
                                 format(config.conn_str))
        super(JobStateManager, self).listen()

    def stop_listening(self):
        super(JobStateManager, self).stop_listening()
        self.publisher.close()

    def log_done(self, job_instance_id):
        logger.debug("Log DONE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            msg = self._update_job_instance_state(
                session, ji, models.JobInstanceStatus.DONE)
        if msg:
            self.publisher.send_string(msg)
        return (ReturnCodes.OK,)

    def log_error(self, job_instance_id, error_message):
        logger.debug("Log ERROR for JI {}, message={}".format(job_instance_id,
                                                              error_message))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            msg = self._update_job_instance_state(
                session, ji, models.JobInstanceStatus.ERROR)
            error = models.JobInstanceErrorLog(job_instance_id=job_instance_id,
                                               description=error_message)
            session.add(error)
        if msg:
            self.publisher.send_string(msg)
        return (ReturnCodes.OK,)

    def log_executor_id(self, job_instance_id, executor_id):
        logger.debug("Log EXECUTOR_ID for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            msg = self._update_job_instance_state(
                session, ji,
                models.JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR)
            self._update_job_instance(session, ji, executor_id=executor_id)
        if msg:
            self.publisher.send_string(msg)
        return (ReturnCodes.OK,)

    def log_heartbeat(self, dag_id):
        with session_scope() as session:
            dag = session.query(task_dag.TaskDagMeta).filter_by(
                dag_id=dag_id).first()
            if dag:
                dag.heartbeat_date = datetime.utcnow()
                session.commit()
            else:
                return (ReturnCodes.NO_RESULTS,)
        return (ReturnCodes.OK,)

    def log_running(self, job_instance_id, nodename, process_group_id):
        logger.debug("Log RUNNING for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            msg = self._update_job_instance_state(
                session, ji, models.JobInstanceStatus.RUNNING)
            ji.nodename = nodename
            ji.process_group_id = process_group_id
        if msg:
            self.publisher.send_string(msg)
        return (ReturnCodes.OK,)

    def log_nodename(self, job_instance_id, nodename=None):
        logger.debug("Log USAGE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance(session, ji, nodename=nodename)
        return (ReturnCodes.OK,)

    def log_usage(self, job_instance_id, usage_str=None,
                  wallclock=None, maxvmem=None, cpu=None, io=None):
        logger.debug("Log USAGE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance(session, ji, usage_str=usage_str,
                                      wallclock=wallclock,
                                      maxvmem=maxvmem, cpu=cpu, io=io)
        return (ReturnCodes.OK,)

    def queue_job(self, job_id):
        logger.debug("Queue Job {}".format(job_id))
        with session_scope() as session:
            job = session.query(models.Job).filter_by(job_id=job_id).first()
            job.transition(models.JobStatus.QUEUED_FOR_INSTANTIATION)
        return (ReturnCodes.OK,)

    def reset_job(self, job_id):
        with session_scope() as session:
            job = session.query(models.Job).filter_by(job_id=job_id).first()
            job.reset()
            session.commit()
        return (ReturnCodes.OK,)

    def reset_incomplete_jobs(self, dag_id):
        with session_scope() as session:
            up_job = """
                UPDATE job
                SET status=:registered_status, num_attempts=0
                WHERE dag_id=:dag_id
                AND job.status!=:done_status
            """
            up_job_instance = """
                UPDATE job_instance
                JOIN job USING(job_id)
                SET job_instance.status=:error_status
                WHERE job.dag_id=:dag_id
                AND job.status!=:done_status
            """
            log_errors = """
                INSERT INTO job_instance_error_log
                    (job_instance_id, description)
                SELECT job_instance_id, 'Job RESET requested' as description
                FROM job_instance
                JOIN job USING(job_id)
                WHERE job.dag_id=:dag_id
                AND job.status!=:done_status
            """
            session.execute(up_job,
                            {"dag_id": dag_id,
                             "registered_status": models.JobStatus.REGISTERED,
                             "done_status": models.JobStatus.DONE})
            session.execute(up_job_instance,
                            {"dag_id": dag_id,
                             "error_status": models.JobInstanceStatus.ERROR,
                             "done_status": models.JobStatus.DONE})
            session.execute(log_errors,
                            {"dag_id": dag_id,
                             "done_status": models.JobStatus.DONE})
            session.commit()
        return (ReturnCodes.OK,)

    def _get_job_instance(self, session, job_instance_id):
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        return job_instance

    def _update_job_instance_state(self, session, job_instance, status_id):
        """Advances the states of job_instance and it's associated Job,
        returning any messages that should be published based on
        the transition"""
        logger.debug("Update JI state {} for  {}".format(status_id,
                                                         job_instance))
        job_instance.transition(status_id)
        job = job_instance.job

        # TODO: Investigate moving this publish logic into some SQLAlchemy-
        # event driven framework. Given the amount of code copying here, to
        # ensure consistenty with committed transactions it doesn't feel like
        # the JobStateManager should be the responsible party on this one.
        #
        # ... see tests/tests_job_state_manager.py for Event example
        if job.status in [models.JobStatus.DONE, models.JobStatus.ERROR_FATAL]:
            to_publish = mogrify(job.dag_id, (job.job_id, job.status))
            return to_publish
        else:
            return None

    def _update_job_instance(self, session, job_instance, **kwargs):
        logger.debug("Update JI  {}".format(job_instance))
        for k, v in kwargs.items():
            setattr(job_instance, k, v)
        return (ReturnCodes.OK,)

    def add_workflow_attribute(self, workflow_id, attribute_type, value):
        workflow_attribute = attribute_models.\
            WorkflowAttribute(workflow_id=workflow_id,
                              attribute_type=attribute_type,
                              value=value)
        with session_scope() as session:
            session.add(workflow_attribute)
            session.commit()
            workflow_attribute_id = workflow_attribute.id
        return (ReturnCodes.OK, workflow_attribute_id)

    def add_workflow_run_attribute(self, workflow_run_id,
                                   attribute_type, value):
        workflow_run_attribute = attribute_models.\
            WorkflowRunAttribute(workflow_run_id=workflow_run_id,
                                 attribute_type=attribute_type,
                                 value=value)
        with session_scope() as session:
            session.add(workflow_run_attribute)
            session.commit()
            workflow_run_attribute_id = workflow_run_attribute.id
        return (ReturnCodes.OK, workflow_run_attribute_id)

    def add_job_attribute(self, job_id, attribute_type, value):
        job_attribute = attribute_models.\
            JobAttribute(job_id=job_id,
                         attribute_type=attribute_type,
                         value=value)
        with session_scope() as session:
            session.add(job_attribute)
            session.commit()
            job_attribute_id = job_attribute.id
        return (ReturnCodes.OK, job_attribute_id)
