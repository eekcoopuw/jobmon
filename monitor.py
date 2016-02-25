import zmq
from . import models
import sqlite3
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from socket import gethostname
import os
import sys
import json
import pickle
import pandas as pd


assert sys.version_info > (3, 0), """
    Sorry, only Python version 3+ are supported at this time"""


class JobMonitor(object):
    """server node.

    Args:
        out_dir (string): full filepath of directory to write server config in
    """

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read"""
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.out_dir = os.path.realpath(self.out_dir)
        try:
            os.makedirs(self.out_dir)
        except:
            pass
        self.port, self.socket = self.start_server()

    @property
    def node_name(self):
        """name of node that server is running on"""
        return gethostname()

    def write_connection_info(self, host, port):
        """dump server connection configuration to network filesystem for
        client nodes to read using json.dump

        Args:
            host (string): node name that server is running on
            port (int): port that server is listening at
        """
        with open('%s/monitor_info.json' % self.out_dir, 'w') as f:
            json.dump({'host': host, 'port': port}, f)

    def start_server(self):
        """configure server and set to listen. returns tuple (port, socket).
        doesn't actually run server."""
        print('Starting server...')
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)  # server blocks on recieve
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name, self.port)  # dump config
        print('Server started.')
        return self.port, self.socket

    def stop_server(self):
        """stops listening at network socket/port."""
        print('Stopping server...')
        self.socket.close()
        print('Server stopped.')
        return True

    def restart_server(self):
        """restart listening at new network socket/port"""
        self.stop_server()
        self.start_server()

    def run(self):
        """Run server. Start consuming registration and status updates from
        jobs."""
        if self.socket.closed:
            print('Server offline, starting...')
            self.start_server()
        keep_alive = True
        while keep_alive:
            msg = self.socket.recv()  # server blocks on recieve
            msg = msg.decode('utf-8')  # json is byte stream. decode to unicode
            try:
                if msg == 'stop':
                    keep_alive = False
                    self.socket.send(b"Monitor stopped")
                    self.stop_server()
                else:
                    msg = json.loads(msg)
                    tocall = getattr(self, msg['action'])
                    if 'kwargs' in msg.keys():
                        kwargs = msg['kwargs']
                    else:
                        kwargs = {}

                    response = tocall(*msg['args'], **kwargs)
                    if self.valid_response(response):
                        p = pickle.dumps(response, protocol=2)
                        self.socket.send(p)
                    else:
                        p = pickle.dumps(
                            (1, b"action has invalid reponse format"),
                            protocol=2)
                        self.socket.send(p)

            except Exception as e:
                print(e)
                p = pickle.dumps((2, b"Uh oh, something went wrong"),
                                 protocol=2)
                self.socket.send(p)

    def valid_response(self, response):
        """validate that action method returns value in expected format.

        Args:
            response (object): any object can be accepted by this method but
                only tuples of the form (response_code, response message) are
                considered valid responses. response_code must be an integer.
                response message can by any byte string.
        """
        if isinstance(response, tuple) & isinstance(response[0], int):
            return True
        else:
            return False

Session = sessionmaker()


class SGEJobMonitor(JobMonitor):
    """server node job status logger.

    Args:
        out_dir (string): full filepath of directory to create job monitor
            sqlite database in.
    """

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read. make sqlite database schema"""
        super().__init__(out_dir)
        self.session = self.create_job_db()

    def create_job_db(self):
        """create sqlite database from models schema"""
        dbfile = '{out_dir}/job_monitor.sqlite'.format(out_dir=self.out_dir)

        def creator():
            return sqlite3.connect(
                'file:{dbfile}?vfs=unix-none'.format(dbfile=dbfile), uri=True)
        eng = sql.create_engine('sqlite://', creator=creator)

        models.Base.metadata.create_all(eng)  # doesn't create if exists
        Session.configure(bind=eng)
        session = Session()

        try:
            models.default_statuses(session)
        except:
            session.rollback()
        return session

    def create_job(self, jid, name, *args, **kwargs):
        """create job entry in database job table.

        Args:
            jid (int): id to add to job table
            name (string): name of job to add to job table

            **kwargs: any keyword args passed through will be treated as insert
                statements for the specified jid where the keys are the column
                names and the values are the column values.
        """
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        if job is None:
            job = models.Job(
                jid=jid,
                name=name,
                current_status=1,
                **kwargs)
            self.session.add(job)
            self.session.commit()
        return (0,)

    def update_job_status(self, jid, status_id):
        """update status of job.

        Args:
            jid (int): job id to update status of
            status_id (int): status id to update job to
        """
        status = models.JobStatus(jid=jid, status=status_id)
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        job.current_status = status_id
        self.session.add_all([status, job])
        self.session.commit()
        return (0,)

    def log_error(self, jid, error):
        """log error for given job id

        Args:
            jid (int): job id to update status of
            error (sting): error message to log
        """
        error = models.JobError(jid=jid, description=error)
        self.session.add(error)
        self.session.commit()
        return (0,)

    def query(self, query):
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
                response = (0, df)
            except ValueError:
                df = pd.DataFrame(columns=(r_proxy.keys()))
                response = (0, df)
            except:
                response = (1, b"query failed to execute")

        except:
            response = (1, b"query failed to execute")

        return response
