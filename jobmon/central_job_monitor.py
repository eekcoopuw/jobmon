import sys
import os
import sqlite3
import pandas as pd
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool

from jobmon import models
from jobmon.publisher import Publisher
from jobmon.responder import Responder, ServerProcType
from jobmon.exceptions import ReturnCodes

Session = sessionmaker()


class CentralJobMonitor(object):
    """Listens for job status update messages,
    writes to sqlite server node.
    server node job status logger.

    Runs as a separate process.

    Args:
        out_dir (string): full filepath of directory to create job monitor
            sqlite database in.
    """

    def __init__(self, out_dir, persistent=True, publish_job_state=True):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read. make sqlite database schema

        Args:
            out_dir (str): where to save the connection json for the zmq socket
                and the sqlite database if the you are creating a persistent
                data store
            persistent (bool, optional): whether to create a persistent sqlite
                database in the file system or just keep it in memory.
                True can only be specified if run in python 3+
        """
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.responder = Responder(out_dir)
        Responder.logger.info("{}: Responder initialized".format(os.getpid()))
        if publish_job_state:
            self.publisher = Publisher(out_dir)
            Publisher.logger.info(
                "{}: Publisher initialized".format(os.getpid()))
        else:
            self.publisher = None

        # Initialize the persistent backend where job-state messages will be
        # recorded
        self.server_proc_type = None
        self.session = self.create_job_db(persistent)
        Responder.logger.info(
            "{}: Backend created. Starting server...".format(os.getpid()))

        self.responder.register_object_actions(self)
        self.responder.start_server(server_proc_type=self.server_proc_type)

    def create_job_db(self, persistent=True):
        """create sqlite database from models schema

        Args:
            persistent (bool, optional): whether to create a persistent sqlite
                database in the file system or just keep it in memory.
                True can only be specified if run in python 3+
        """
        if persistent:
            assert sys.version_info > (3, 0), """
                Sorry, only Python version 3+ is supported at this time"""
            logmsg = "{}: Creating persistent backend".format(os.getpid())
            Responder.logger.info(logmsg)

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

        models.Base.metadata.create_all(eng)  # doesn't create if exists
        Session.configure(bind=eng, autocommit=False)
        session = Session()

        try:
            models.load_default_statuses(session)
        except IntegrityError:
            Responder.logger.info(
                "Status table already loaded. If you intended to use a fresh "
                "database, you'll have to delete the "
                "old database manually {}".format(dbfile))
            session.rollback()
        return session

    def responder_proc_is_alive(self):
        return self.responder.server_proc.is_alive()

    def stop_responder(self):
        return self.responder.stop_server()

    def jobs_with_status(self, status_id):
        # TODO this query is maybe not right once/if we implement retries
        jobs = (
            self.session.query(models.Job).
            filter(models.JobInstance.jid == models.Job.jid).
            filter(models.JobInstance.current_status == status_id).all())
        return jobs

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
        r_proxy = self.session.execute(q1)
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
        r_proxy = self.session.execute(q2)
        df = r_proxy_2_df(r_proxy)
        df.to_csv(os.path.join(self.out_dir, "job_status_report.csv"))

    def _action_get_job_information(self, jid):
        job = self.session.query(models.Job).filter_by(jid=jid)
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

    def _action_register_job(self, name=None, runfile=None, job_args=None):
        job = models.Job(
            name=name,
            runfile=runfile,
            args=job_args)
        self.session.add(job)
        self.session.commit()
        return 0, job.jid

    def _action_get_job_instance_information(self, job_instance_id):
        job_instance = self.session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id)
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
                args=kwargs.get("args")
            )[1]
        job_instance = models.JobInstance(
            job_instance_id=job_instance_id,
            jid=jid,
            current_status=models.Status.SUBMITTED,
            **kwargs)
        self.session.add(job_instance)
        self.session.commit()
        return (ReturnCodes.OK, job_instance.job_instance_id)

    def _action_update_job_instance_status(self, job_instance_id, status_id):
        """update status of job.

        Args:
            job_instance_id (int): job instance id to update status of
            status_id (int): status id to update job to
        """
        # publish status update to any subscribers
        if self.publisher:
            self.publisher.publish_info(
                "job_instance_status", (job_instance_id, status_id))

        job_instance = self.session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        job_instance.current_status = status_id
        status = models.JobInstanceStatus(job_instance_id=job_instance_id,
                                          status=status_id)
        self.session.add_all([status, job_instance])
        self.session.commit()
        return (ReturnCodes.OK, job_instance_id, status_id)

    def _action_update_job_instance_usage(
            self, job_instance_id, *args, **kwargs):
        job_instance = self.session.query(models.JobInstance).filter_by(
            job_instance_id=job_instance_id).first()
        for k, v in kwargs.items():
            setattr(job_instance, k, v)
        self.session.add(job_instance)
        self.session.commit()
        return (ReturnCodes.OK,)

    def _action_log_job_instance_error(self, job_instance_id, error):
        """log error for given job id

        Args:
            job_instance_id (int): sge_id to update status of
            error (string): error message to log
        """
        error = models.JobInstanceError(job_instance_id=job_instance_id,
                                        description=error)
        self.session.add(error)
        self.session.commit()
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
            r_proxy = self.session.execute(query)

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
