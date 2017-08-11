import logging
import sys
import os
import sqlite3
import pandas as pd
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool

from jobmon import models
from jobmon.app import app, db
from jobmon.publisher import Publisher, PublisherTopics
from jobmon.responder import Responder, ServerProcType
from jobmon.exceptions import ReturnCodes

from jobmon.setup_logger import setup_logger

logger = setup_logger("jobmon", path="server_logging.yaml")

class CentralJobMonitor(object):
    """Listens for job status update messages,
    writes to sqlite server node.
    server node job status logger.

    Runs as a separate process.

    Args:
        out_dir (string): full filepath of directory to create job monitor
            sqlite database in.
    """

    def __init__(self, out_dir, persistent=True, port=None, conn_str=None,
                 publish_job_state=True, publisher_port=None):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read. make sqlite database schema

        Args:
            out_dir (str): where to save the connection json for the zmq socket
                and the sqlite database if the you are creating a persistent
                data store
            persistent (bool, optional): whether to create a persistent sqlite
                database in the file system or just keep it in memory.
                True can only be specified if run in python 3+
            port (int): Port that the monitor should listen on. If None
                (default), the system will choose the port
            publish_job_state (bool, optional): whether to use a zmq publisher
                to broadcast job status updates
            publisher_port (int): Port that the publisher should listen on. If
                None (default), the system will choose the port
        """
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.conn_str = conn_str
        self.responder = Responder(out_dir, port=port)
        logmsg = "{}: Responder initialized".format(os.getpid())
        logger.info(logmsg)

        # Initialize the persistent backend where job-state messages will be
        # recorded
        self.server_proc_type = ServerProcType.SUBPROCESS
        self.Session = sessionmaker()
        self.create_job_db(persistent)
        logmsg = "{}: Backend created. Starting server...".format(os.getpid())
        logger.info(logmsg)

        if publish_job_state:
            self.publisher = Publisher(out_dir, port=publisher_port)
            self.publisher.start_publisher()
            logger.info(
                "{}: Publisher initialized".format(os.getpid()))
        else:
            self.publisher = None

        self.responder.register_object_actions(self)
        self.responder.start_server(server_proc_type=self.server_proc_type)

    def create_job_db(self, persistent=True):
        """create sqlite database from models schema

        Args:
            persistent (bool, optional): whether to create a persistent sqlite
                database in the file system or just keep it in memory.
                True can only be specified if run in python 3+
        """
        if self.conn_str:
            eng = sql.create_engine(self.conn_str, pool_recycle=300,
                                    pool_size=20, max_overflow=100,
                                    pool_timeout=120)
            dbfile = self.conn_str
        else:
            if persistent:
                assert sys.version_info > (3, 0), """
                    Sorry, only Python version 3+ is supported at this time"""
                logmsg = "{}: Creating persistent backend".format(os.getpid())
                logger.info(logmsg)

                dbfile = '{out_dir}/job_monitor.sqlite'.format(
                    out_dir=self.out_dir)

                def creator():
                    return sqlite3.connect(
                        'file:{dbfile}?vfs=unix-none'.format(dbfile=dbfile),
                        uri=True)
                eng = sql.create_engine('sqlite://', creator=creator)
                self.server_proc_type = ServerProcType.SUBPROCESS
            else:
                eng = sql.create_engine(
                    'sqlite://',
                    connect_args={'check_same_thread': False},
                    poolclass=StaticPool)
                self.server_proc_type = ServerProcType.THREAD

        self.Session.configure(bind=eng, autocommit=False)
        db.Model.metadata.create_all(bind=eng)  # doesn't create if exists
        return True

    def load_db_statuses(self):
        if self.conn_str:
            try:
                with app.app_context():
                    models.load_default_statuses()
            except:
                pass
        else:
            session = self.Session()

            try:
                models.load_default_statuses(session)
            except IntegrityError:
                logger.info(
                    "Status table already loaded. If you intended to use a "
                    "fresh database, you'll have to delete the "
                    "old database manually")
                session.rollback()
            session.close()
        return True

    def responder_proc_is_alive(self):
        return self.responder.server_proc.is_alive()

    def stop_responder(self):
        return self.responder.stop_server()

    def stop_publisher(self):
        return self.publisher.stop_publisher()

    def jobs_with_status(self, status_id):
        # TODO this query is maybe not right once/if we implement retries
        session = self.Session()
        jobs = (
            session.query(models.Job).
            filter(models.JobInstance.jid == models.Job.jid).
            filter(models.JobInstance.current_status == status_id).all())
        session.close()
        return jobs

    def _action_get_jobs_with_status(self, status_id):
        jobs = self.jobs_with_status(status_id)
        length = len(jobs)
        if length == 0:
            return (ReturnCodes.NO_RESULTS,
                    "Found no job with status {}".format(status_id))
        else:
            return (ReturnCodes.OK, [job.jid for job in jobs])

    def generate_report(self):
        """save a csv representation of the database"""

        def r_proxy_2_df(r_proxy):

            # load dataframe
            try:
                df = pd.DataFrame(r_proxy.fetchall())
                df.columns = r_proxy.keys()
            except ValueError:
                df = pd.DataFrame(columns=(r_proxy.keys()))
            return df

        q1 = """
        SELECT
            *
        FROM
            job
        LEFT JOIN
            job_instance USING (jid)
        LEFT JOIN
            job_instance_error USING (job_instance_id)
        """
        session = self.Session()
        r_proxy = session.execute(q1)
        df = r_proxy_2_df(r_proxy)
        df.to_csv(os.path.join(self.out_dir, "job_report.csv"))

        q2 = """
        SELECT
            *
        FROM
            job_instance_status jis
        LEFT JOIN
            status s ON s.id = jis.status
        """
        r_proxy = session.execute(q2)
        df = r_proxy_2_df(r_proxy)
        df.to_csv(os.path.join(self.out_dir, "job_status_report.csv"))
        session.close()

    def _action_get_job_information(self, jid):
        session = self.Session()
        job = session.query(models.Job).filter_by(jid=jid)
        result = job.all()
        length = len(result)
        if length == 0:
            return (ReturnCodes.NO_RESULTS,
                    "Found no job with jid {}".format(jid))
        elif length == 1:
            # Problem. Can't just pass in result[0].__dict__ to be serialized
            # because it contains sqlalcehmy objects that are not serializable.
            # So construct a "safe" dict
            return (ReturnCodes.OK, result[0].to_wire_format_dict())
        else:
            return (ReturnCodes.GENERIC_ERROR,
                    "Found too many results ({}) for jid {}".format(length,
                                                                    jid))

    def _action_register_batch(self, name=None, user=None):
        batch = models.Batch(
            name=name,
            user=user)
        session = self.Session()
        session.add(batch)
        session.commit()
        bid = batch.batch_id
        session.close()
        logger.debug("Register batch done, id = {}".format(bid))
        return ReturnCodes.OK, bid

    def _action_register_job(self, name=None, runfile=None, job_args=None,
                             batch_id=None):
        job = models.Job(
            name=name,
            runfile=runfile,
            args=job_args,
            batch_id=batch_id)
        session = self.Session()
        session.add(job)
        session.commit()
        jid = job.jid
        session.close()
        return ReturnCodes.OK, jid

    def _action_get_job_instance_information(self, job_instance_id):
        session = self.Session()
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id)
        session.close()
        result = job_instance.all()
        length = len(result)
        if length == 0:
            return (ReturnCodes.NO_RESULTS,
                    "Found no job with job_instance_id {}".format(
                        job_instance_id))
        elif length == 1:
            # Problem. Can't just pass in result[0].__dict__ to be serialized
            # because it contains sqlalcehmy objects that are not serializable.
            # So construct a "safe" dict
            return (ReturnCodes.OK, result[0].to_wire_format_dict())
        else:
            return (ReturnCodes.GENERIC_ERROR,
                    "Found too many results ({}) for job_instance_id {}".
                    format(length, job_instance_id))

    def _action_register_job_instance(
            self, job_instance_id, jid=None, *args, **kwargs):
        """create job entry in database job table.

        Args:
            job_instance_id (int): unique job id assigned by executor
            jid (int, optional): auto incrementing id assigned by
                central_job_monitor backend sqlite database. If not specified
                a new entry will be created.

            **kwargs: any keyword args passed through will be treated as insert
                statements for the specified job_instance_id where the keys are
                the column names and the values are the column values.
        """
        # if jid is not provided, create a new entry
        if jid is None:
            jid = self._action_register_job(
                name=kwargs.get("name"),
                runfile=kwargs.get("runfile"),
                job_args=kwargs.get("args")
            )[1]
        job_instance = models.JobInstance(
            job_instance_id=job_instance_id,
            jid=jid,
            current_status=models.Status.SUBMITTED,
            **kwargs)
        session = self.Session()
        session.add(job_instance)
        session.commit()
        ji_id = job_instance.job_instance_id
        session.close()
        return (ReturnCodes.OK, ji_id)

    def _action_update_job_instance_status(self, job_instance_id, status_id):
        """update status of job.

        Args:
            job_instance_id (int): job instance id to update status of
            status_id (int): status id to update job to
        """
        # update sge_job statuses
        session = self.Session()
        session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).update(
                {'current_status': status_id})
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        jid = job_instance.jid
        status = models.JobInstanceStatus(job_instance_id=job_instance_id,
                                          status=status_id)
        session.add(status)
        session.commit()
        session.close()

        if self.publisher:
            logger.debug(
                "Publishing job instance status update {id}:{s}".format(id=job_instance_id, s=status_id))
            self.publisher.publish_info(
                PublisherTopics.JOB_STATE.value,
                {jid: {"job_instance_id": job_instance_id,
                       "job_instance_status_id": status_id}})
        else:
            logger.debug("No publisher, not publishing job instance status update {id}:{s}".format(id=job_instance_id, s=status_id)))

        return (ReturnCodes.OK, job_instance_id, status_id)

    def _action_update_job_instance_usage(
            self, job_instance_id, *args, **kwargs):
        session = self.Session()
        job_instance = session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        for k, v in kwargs.items():
            setattr(job_instance, k, v)
        session.add(job_instance)
        session.commit()
        session.close()
        return (ReturnCodes.OK,)

    def _action_log_job_instance_error(self, job_instance_id, error):
        """log error for given job id

        Args:
            job_instance_id (int): sge_id to update status of
            error (string): error message to log
        """
        error = models.JobInstanceError(job_instance_id=job_instance_id,
                                        description=error)
        session = self.Session()
        session.add(error)
        session.commit()
        session.close()
        return (ReturnCodes.OK,)

    def _action_query(self, query):
        # TODO: Deprecate this action or at least refactor in such a way that
        # responses are returnable via JSON. I don't know that
        # we want to resurrect pickle as the serialization format, and
        # I don't know that we really want message-passing to be able to
        # generically 'query' the database... seems like we would want to
        # expose more targeted actions on the DB to requesters, and keep
        # large open-ended 'queries' server-side
        """execute raw sql query on sqlite database

        Args:
            query (string): raw sql query string to execute on sqlite database
        """
        try:
            # run query
            session = self.Session()
            r_proxy = session.execute(query)
            session.close()

            # load dataframe
            try:
                df = pd.DataFrame(r_proxy.fetchall())
                df.columns = r_proxy.keys()
                response = (ReturnCodes.OK, df.to_dict(orient='records'))
            except ValueError:
                df = pd.DataFrame(columns=(r_proxy.keys()))
                response = (ReturnCodes.OK, df.to_dict(orient='records'))
            except Exception as e:
                response = (1, "dataframe failed to load {}".format(e))

        except Exception as e:
            response = (1, "query failed to execute {}".format(e).encode())

        return response
