import logging
from datetime import datetime

from flask import jsonify

from jobmon import models
from jobmon.database import session_scope
from jobmon.exceptions import ReturnCodes
from jobmon.pubsub_helpers import mogrify
from jobmon.reply_server import ReplyServer
from jobmon.meta_models import task_dag
from jobmon.workflow.workflow import WorkflowDAO
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus

# logging does not work well in python < 2.7 with Threads,
# see https://docs.python.org/2/library/logging.html
# Logging has to be set up BEFORE the Thread
# Therefore see tests/conf_test.py
logger = logging.getLogger(__name__)


class JobStateManager(ReplyServer):

    app = ReplyServer.app

    def __init__(self):
        super(JobStateManager, self).__init__()

    @app.route('/job', methods=['POST'])
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
            job_id = job.job_id
        return jsonify(return_code=ReturnCodes.OK, job_id=job_id)

    @app.route('/task_dag', methods=['POST'])
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
        return jsonify(return_code=ReturnCodes.OK, dag_id=dag_id)

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

    @app.route('/job_instance', methods=['POST'])
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
        return jsonify(return_code=ReturnCodes.OK, job_instance_id=ji_id)

    @app.route('/workflow', methods=['POST'])
    def add_workflow(self, dag_id, workflow_args, workflow_hash, name, user,
                     description=""):
        wf = WorkflowDAO(dag_id=dag_id, workflow_args=workflow_args,
                         workflow_hash=workflow_hash, name=name, user=user,
                         description=description)
        with session_scope() as session:
            session.add(wf)
            session.commit()
            wf_dct = wf.to_wire()
        return jsonify(return_code=ReturnCodes.OK, workflow_dct=wf_dct)

    @app.route('/workflow_run', methods=['POST'])
    def add_workflow_run(self, workflow_id, user, hostname, pid, stderr,
                         stdout, project, slack_channel):
        wfr = WorkflowRunDAO(workflow_id=workflow_id,
                             user=user,
                             hostname=hostname,
                             pid=pid,
                             stderr=stderr,
                             stdout=stdout,
                             project=project,
                             slack_channel=slack_channel)
        with session_scope() as session:
            workflow = session.query(WorkflowDAO).\
                filter(WorkflowDAO.id == workflow_id).first()
            # Set all previous runs to STOPPED
            for run in workflow.workflow_runs:
                run.status = WorkflowRunStatus.STOPPED
            session.add(wfr)
            session.commit()
            wfr_id = wfr.id
        return jsonify(return_code=ReturnCodes.OK, workflow_run_id=wfr_id)

    @app.route('/workflow', methods=['POST'])
    def update_workflow(self, wf_id, status):
        with session_scope() as session:
            wf = session.query(WorkflowDAO).\
                filter(WorkflowDAO.id == wf_id).first()
            wf.status = status
            wf.status_date = datetime.utcnow()
            session.commit()
            wf_dct = wf.to_wire()
        return jsonify(code=ReturnCodes.OK, workflow_dct=wf_dct)

    @app.route('/workflow_run', methods=['POST'])
    def update_workflow_run(self, wfr_id, status):
        with session_scope() as session:
            wfr = session.query(WorkflowRunDAO).\
                filter(WorkflowRunDAO.id == wfr_id).first()
            wfr.status = status
            wfr.status_date = datetime.utcnow()
            session.commit()
        return jsonify(return_code=ReturnCodes.OK, status=status)

    @app.route('/workflow', methods=['GET'])
    def is_workflow_running(self, workflow_id):
        """Check if a previous workflow run for your user is still running """
        with session_scope() as session:
            wf_run = (session.query(WorkflowRunDAO).filter_by(
                workflow_id=workflow_id, status=WorkflowRunStatus.RUNNING,
            ).order_by(WorkflowRunDAO.id.desc()).first())
            if not wf_run:
                return jsonify(return_code=ReturnCodes.OK,
                               status=False, workflow_run_id=None,
                               hostname=None, pid=None, user=None)
            wf_run_id = wf_run.id
            hostname = wf_run.hostname
            pid = wf_run.pid
            user = wf_run.user
        return jsonify(return_code=ReturnCodes.OK, status=True,
                       workflow_run_id=wf_run_id, hostname=hostname,
                       pid=pid, user=user)

    @app.route('/workflow_run', methods=['GET'])
    def get_sge_ids_of_previous_workflow_run(workflow_run_id):
        with session_scope() as session:
            jis = session.query(models.JobInstance).filter_by(
                workflow_run_id=workflow_run_id).all()
            sge_ids = [ji.executor_id for ji in jis]
        return jsonify(return_code=ReturnCodes.OK, sge_ids=sge_ids)

    @app.route('/job_instance', methods=['POST'])
    def log_done(self, job_instance_id):
        logger.debug("Log DONE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            msg = self._update_job_instance_state(
                session, ji, models.JobInstanceStatus.DONE)
        if msg:
            self.publisher.send_string(msg)
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job_instance', methods=['POST'])
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
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job_instance', methods=['POST'])
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
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/task_dag', methods=['POST'])
    def log_heartbeat(self, dag_id):
        with session_scope() as session:
            dag = session.query(task_dag.TaskDagMeta).filter_by(
                dag_id=dag_id).first()
            if dag:
                dag.heartbeat_date = datetime.utcnow()
                session.commit()
            else:
                return jsonify(return_code=ReturnCodes.NO_RESULTS,)
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job_instance', methods=['POST'])
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
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job_instance', methods=['POST'])
    def log_nodename(self, job_instance_id, nodename=None):
        logger.debug("Log USAGE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance(session, ji, nodename=nodename)
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job_instance', methods=['POST'])
    def log_usage(self, job_instance_id, usage_str=None,
                  wallclock=None, maxvmem=None, cpu=None, io=None):
        logger.debug("Log USAGE for JI {}".format(job_instance_id))
        with session_scope() as session:
            ji = self._get_job_instance(session, job_instance_id)
            self._update_job_instance(session, ji, usage_str=usage_str,
                                      wallclock=wallclock,
                                      maxvmem=maxvmem, cpu=cpu, io=io)
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job', methods=['POST'])
    def queue_job(self, job_id):
        logger.debug("Queue Job {}".format(job_id))
        with session_scope() as session:
            job = session.query(models.Job).filter_by(job_id=job_id).first()
            job.transition(models.JobStatus.QUEUED_FOR_INSTANTIATION)
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/job', methods=['POST'])
    def reset_job(self, job_id):
        with session_scope() as session:
            job = session.query(models.Job).filter_by(job_id=job_id).first()
            job.reset()
            session.commit()
        return jsonify(return_code=ReturnCodes.OK,)

    @app.route('/task_dag', methods=['POST'])
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
        return jsonify(return_code=ReturnCodes.OK,)

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
        return jsonify(return_code=ReturnCodes.OK,)
