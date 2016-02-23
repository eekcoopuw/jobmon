import zmq
from . import models
import sqlite3
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from socket import gethostname
import os
import sys
import json


assert sys.version_info > (3, 0), """
    Sorry, only Python version 3+ are supported at this time"""


Session = sessionmaker()


class JobMonitor(object):
    """server node job status logger.

    Args:
        out_dir (string): full filepath of directory to create job monitor
            sqlite database in.
    """

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read. make sqlite database schema"""
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.out_dir = os.path.realpath(self.out_dir)
        try:
            os.makedirs(self.out_dir)
        except:
            pass
        self.port, self.socket = self.start_server()
        self.session = self.create_job_db()  # don't like this

    def create_job_db(self):
        # I think this is not the best implementation to have a create_job_db
        # method return a session object. what happens if you want to restart
        # the server but resume using an old database.
        dbfile = '{out_dir}/job_monitor.sqlite'.format(out_dir=self.out_dir)

        def creator():
            return sqlite3.connect(
                'file:{dbfile}?vfs=unix-none'.format(dbfile=dbfile), uri=True)
        eng = sql.create_engine('sqlite://', creator=creator)

        # what happens if this already exists?
        models.Base.metadata.create_all(eng)
        Session.configure(bind=eng)
        session = Session()

        try:
            models.default_statuses(session)
        except:
            session.rollback()
        return session

    def create_job(self, jid, name, *args, **kwargs):
        """create job entry in database job table"""
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        if job is None:
            job = models.Job(
                jid=jid,
                name=name,
                current_status=1,
                **kwargs)
            self.session.add(job)
            self.session.commit()
        return job

    def update_job_status(self, jid, status_id):
        """update status of job"""
        status = models.JobStatus(jid=jid, status=status_id)
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        job.current_status = status_id
        self.session.add_all([status, job])
        self.session.commit()
        return status

    def log_error(self, jid, error):
        """log an error"""
        error = models.JobError(jid=jid, description=error)
        self.session.add(error)
        self.session.commit()
        return error

    def node_name(self):
        """return node name. perfect candidate for a property decorator"""
        return gethostname()

    def write_connection_info(self, host, port):
        """dump server connection configuration to network filesystem for
        client nodes to read"""
        with open('%s/monitor_info.json' % self.out_dir, 'w') as f:
            json.dump({'host': host, 'port': port}, f)

    def start_server(self):
        """configure server and set to listen. returns tuple (port, socket).
        doesn't actually run server despite having start in the name."""
        print('Starting server...')
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)  # server blocks on recieve
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name(), self.port)  # dump config
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
        jobs. maybe this method should have a resume option where it reads
        a preexisting config file instead of binding to a random port every
        time."""
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
                    tocall(*msg['args'], **kwargs)

                    # should send whatever tocall() returns
                    self.socket.send(b"OK")
            except Exception as e:
                print(e)
                self.socket.send(b"Uh oh, something went wrong")
