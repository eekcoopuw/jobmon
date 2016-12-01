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

from jobmon.setup_logger import setup_logger

assert sys.version_info > (3, 0), """
    Sorry, only Python version 3+ are supported at this time"""


class Server(object):
    """This really is a server, in that there is one of these, it listens on a
    Receiver object (a zmq channel) and does stuff as a result of those
    commands.  A singleton in the directory. Runs as a separate process, not in
    the same process that started the qmaster.

    Args:
        out_dir (string): full filepath of directory to write server config in
    """
    logger = None

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read"""
        if not Server.logger:
            Server.logger = setup_logger('central_monitor',
                                         'central_monitor_logging.yaml')
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.out_dir = os.path.realpath(self.out_dir)
        try:
            os.makedirs(self.out_dir)
        except FileExistsError:  # It throws if the directory already exists!
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
        filename = '%s/monitor_info.json' % self.out_dir
        Server.logger.debug('{}: Writing connection info to {}'.format(os.getpid(), filename))
        with open(filename, 'w') as f:
            json.dump({'host': host, 'port': port}, f)

    def start_server(self):
        """configure server and set to listen. returns tuple (port, socket).
        doesn't actually run server."""
        Server.logger.info('{}: Starting server...'.format(os.getpid()))
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)  # server blocks on receive
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name, self.port)  # dump config
        Server.logger.info('Server started.')
        return self.port, self.socket

    def stop_server(self):
        """stops listening at network socket/port."""
        Server.logger.info('Stopping server...')
        os.remove('%s/monitor_info.json' % self.out_dir)
        self.socket.close()
        Server.logger.info('Server stopped.')
        return True

    def restart_server(self):
        """restart listening at new network socket/port"""
        Server.logger.info('Restarting server...')
        self.stop_server()
        self.start_server()

    def run(self):
        """Run server. Start consuming registration and status updates from
        jobs. Use introspection to call the handler method"""
        Server.logger.debug("{}: Server run loop starting".format(os.getpid()))
        if self.socket.closed:
            Server.logger.info('Server offline, starting...')
            self.start_server()
        keep_alive = True
        while keep_alive:
            msg = self.socket.recv()  # server blocks on receive
            msg = msg.decode('utf-8')  # json is byte stream. decode to unicode
            Server.logger.debug("{}: Server received message {} ".format(os.getpid(), msg))
            try:
                if msg == 'stop':
                    keep_alive = False
                    Server.logger.info("{}: Server Stopping".format(os.getpid()))
                    p = pickle.dumps((0, b"Server stopping"), protocol=2)
                    self.socket.send(p)
                    self.stop_server()
                else:
                    # An actual application message, use introspection to find the handler
                    msg = json.loads(msg)
                    tocall = getattr(self, msg['action'])
                    if 'kwargs' in msg.keys():
                        kwargs = msg['kwargs']
                    else:
                        kwargs = {}

                    response = tocall(*msg['args'], **kwargs)
                    if self.is_valid_response(response):
                        p = pickle.dumps(response, protocol=2)
                        Server.logger.debug('{}: Server sending response {}'.format(os.getpid(), response))
                        self.socket.send(p)
                    else:
                        p = pickle.dumps(
                            (1, b"action has invalid response format"),
                            protocol=2)
                        self.socket.send(p)
            except Exception as e:
                Server.logger.error('{}: Server sending "generic problem" error {}'.format(os.getpid(), e))
                print('{}: Server sending "generic problem" error {}'.format(os.getpid(), e))
                p = pickle.dumps((2, b"Uh ooooooh, something went wrong, error"),
                                 protocol=2)
                self.socket.send(p)

    def is_valid_response(self, response):
        """validate that action method returns value in expected format.

        Args:
            response (object): any object can be accepted by this method but
                only tuples of the form (response_code, response message) are
                considered valid responses. response_code must be an integer.
                response message can by any byte string.
        """
        return isinstance(response, tuple) and isinstance(response[0], int)

    def alive(self):
        Server.logger.debug("{}: Server received is_alive?".format(os.getpid()))
        return 0, "alive pid =".format(os.getpid())


Session = sessionmaker()


class CentralJobMonitor(Server):
    """Listens for job status update messages,
    writes to sqllite server node.
    server node job status logger.

    Runs as a separate process.

    Args:
        out_dir (string): full filepath of directory to create job monitor
            sqlite database in.
    """

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read. make sqlite database schema"""
        super(CentralJobMonitor, self).__init__(out_dir)
        Server.logger.debug("{}: Initialize CentralJobMonitor in '{}'".format(os.getpid(), out_dir))
        self.session = self.create_job_db()
        Server.logger.debug("   {}: Initialize CentralJobMonitor complete".format(os.getpid()))

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

    def create_job(self, jid=None):
        job = models.Job(current_status=1)
        self.session.add(job)
        self.session.commit()
        return 0, job.jid

    def create_sgejob(self, name, jid=None, *args, **kwargs):
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

    def update_job_usage(self, jid, *args, **kwargs):
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        for k, v in kwargs.items():
            setattr(job, k, v)
        self.session.add(job)
        self.session.commit()
        return (0,)

    def log_error(self, jid, error):
        """log error for given job id

        Args:
            jid (int): job id to update status of
            error (string): error message to log
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
            except Exception as e:
                response = (1, "dataframe failed to load {}".format(e).encode())

        except Exception as e:
            response = (1, "query failed to execute {}".format(e).encode())

        return response

if __name__ == "__main__":
        m = CentralJobMonitor(sys.argv[1])
        m.run()
