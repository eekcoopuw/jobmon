import os
import sqlite3
import pandas as pd
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from jobmon import models
from jobmon.responder import Responder
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

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read. make sqlite database schema"""
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.responder = Responder(out_dir)
        logmsg = "{}: Responder initialized".format(os.getpid())
        Responder.logger.info(logmsg)

        # Initialize the persistent backend where job-state messages will be
        # recorded
        logmsg = "{}: Creating persistent backend".format(os.getpid())
        Responder.logger.info(logmsg)
        self.session = self.create_job_db()
        logmsg = "{}: Backend created. Starting server...".format(os.getpid())
        Responder.logger.info(logmsg)

        self.responder.register_object_actions(self)
        self.responder.start_server()

    def create_job_db(self):
        """create sqlite database from models schema"""
        dbfile = '{out_dir}/job_monitor.sqlite'.format(out_dir=self.out_dir)

        def creator():
            return sqlite3.connect(
                'file:{dbfile}?vfs=unix-none'.format(dbfile=dbfile), uri=True)
        eng = sql.create_engine('sqlite://', creator=creator)

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
        jobs = (
            self.session.query(models.Job).filter_by(current_status=status_id))
        return jobs

    def _action_get_job_information(self, monitored_jid):
        job = self.session.query(models.Job).filter_by(
            monitored_jid=monitored_jid)
        result = job.all()
        length = len(result)
        if length == 0:
            return (ReturnCodes.NO_RESULTS,
                    "Found no job with sge_id {}".format(monitored_jid))
        elif length == 1:
            # Problem. Can't just pass in result[0].__dict__ to be serialized
            # because it contains sqlalcehmy objects that are not serializable.
            # So construct a "safe" dict
            return (ReturnCodes.OK, result[0].to_wire_format_dict())
        else:
            return (ReturnCodes.GENERIC_ERROR,
                    "Found too many results ({}) for monitored_jid {}".format(
                        length, monitored_jid))

    def _action_register_job(self, name=None, runfile=None, args=None):
        job = models.Job(
            current_status=models.Status.SUBMITTED,
            name=name,
            runfile=runfile,
            args=args)
        self.session.add(job)
        self.session.commit()
        return 0, job.monitored_jid

    def _action_update_job_status(self, jid, status_id):
        """update status of job.

        Args:
            jid (int): job id to update status of
            status_id (int): status id to update job to
        """
        status = models.JobStatus(monitored_jid=jid, status=status_id)
        job = self.session.query(models.Job).filter_by(
            monitored_jid=jid).first()
        job.current_status = status_id
        self.session.add_all([status, job])
        self.session.commit()
        return (ReturnCodes.OK, jid, status_id)

    def _action_get_sgejob_information(self, sge_id):
        sgejob = self.session.query(models.SGEJob).filter_by(sge_id=sge_id)
        result = sgejob.all()
        length = len(result)
        if length == 0:
            return (ReturnCodes.NO_RESULTS,
                    "Found no job with sge_id {}".format(sge_id))
        elif length == 1:
            # Problem. Can't just pass in result[0].__dict__ to be serialized
            # because it contains sqlalcehmy objects that are not serializable.
            # So construct a "safe" dict
            return (ReturnCodes.OK, result[0].to_wire_format_dict())
        else:
            return (ReturnCodes.GENERIC_ERROR,
                    "Found too many results ({}) for sge_id {}".format(
                        length, sge_id))

    def _action_register_sgejob(self, sge_id, name, monitored_jid=None,
                                *args, **kwargs):
        """create job entry in database job table.

        Args:
            sge_id (int): unique job id assigned by univa grid engine
            name (string): name of job to add to job table
            monitored_jid (int, optional): auto incrementing id assigned by
                central_job_monitor backend sqlite database. If not specified
                a new entry will be created.

            **kwargs: any keyword args passed through will be treated as insert
                statements for the specified jid where the keys are the column
                names and the values are the column values.
        """
        # if monitored_jid is not provided, create a new entry
        if monitored_jid is None:
            monitored_jid = self._action_register_job(
                name=kwargs.get("name"),
                runfile=kwargs.get("runfile"),
                args=kwargs.get("args")
            )[1]
        sgejob = models.SGEJob(
            sge_id=sge_id,
            monitored_jid=monitored_jid,
            name=name,
            current_status=models.Status.SUBMITTED,
            **kwargs)
        self.session.add(sgejob)
        self.session.commit()
        return (ReturnCodes.OK, sgejob.monitored_jid)

    def _action_update_sgejob_status(self, sge_id, status_id):
        """update status of job.

        Args:
            jid (int): job id to update status of
            status_id (int): status id to update job to
        """
        # update sge_job statuses
        sgejob = self.session.query(models.SGEJob).filter_by(
            sge_id=sge_id).first()
        sgejob.current_status = status_id
        status = models.SGEJobStatus(sge_id=sge_id, status=status_id)
        self.session.add_all([status, sgejob])
        self.session.commit()

        # update job statuses
        self._action_update_job_status(sgejob.monitored_jid, status_id)

        return (ReturnCodes.OK, sge_id, status_id)

    def _action_update_sgejob_usage(self, sge_id, *args, **kwargs):
        sgejob = self.session.query(models.SGEJob).filter_by(
            sge_id=sge_id).first()
        for k, v in kwargs.items():
            setattr(sgejob, k, v)
        self.session.add(sgejob)
        self.session.commit()
        return (ReturnCodes.OK,)

    def _action_log_error(self, jid, error):
        """log error for given job id

        Args:
            jid (int): job id to update status of
            error (string): error message to log
        """
        error = models.JobError(monitored_jid=jid, description=error)
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
                response = (ReturnCodes.OK, df)
            except ValueError:
                df = pd.DataFrame(columns=(r_proxy.keys()))
                response = (ReturnCodes.OK, df.to_dict())
            except Exception as e:
                response = (1,
                            "dataframe failed to load {}".format(e))

        except Exception as e:
            response = (1, "query failed to execute {}".format(e).encode())

        return response
