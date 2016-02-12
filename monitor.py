import zmq
import models
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from socket import gethostname
import os
import json

Session = sessionmaker()


class JobMonitor(object):

    def __init__(self, out_dir):
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.port, self.socket = self.start_server()
        try:
            os.makedirs(self.out_dir)
        except:
            pass
        self.session = self.create_job_db()

    def create_job_db(self):
        eng = sql.create_engine(
            'sqlite:///%s/job_monitor.sqlite' % self.out_dir)
        models.Base.metadata.create_all(eng)
        Session.configure(bind=eng)
        session = Session()
        try:
            models.default_statuses(session)
        except:
            session.rollback()
        return session

    def create_job(self, jid, name, runfile="", args=[]):
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        if job is None:
            job = models.Job(
                jid=jid,
                name=name,
                runfile=runfile,
                args=json.dumps(args),
                current_status=1)
            self.session.add(job)
            self.session.commit()
        return job

    def update_job_status(self, jid, status_id):
        status = models.JobStatus(jid=jid, status=status_id)
        job = self.session.query(models.Job).filter_by(jid=jid).first()
        job.current_status = status_id
        self.session.add_all([status, job])
        self.session.commit()
        return status

    def log_error(self, jid, error):
        error = models.JobError(jid=jid, description=error)
        self.session.add(error)
        self.session.commit()
        return error

    def node_name(self):
        return gethostname()

    def write_connection_info(self, host, port):
        with open('%s/monitor_info.json' % self.out_dir, 'w') as f:
            json.dump({'host': host, 'port': port}, f)

    def start_server(self):
        print 'Starting server...'
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name(), self.port)
        print 'Server started.'
        return self.port, self.socket

    def stop_server(self):
        print 'Stopping server...'
        self.socket.close()
        print 'Server stopped.'
        return True

    def restart_server(self):
        self.stop_server()
        self.start_server()

    def run(self):
        if self.socket.closed:
            print 'Server offline, starting...'
            self.start_server()
        keep_alive = True
        while keep_alive:
            msg = self.socket.recv()
            try:
                if msg == 'stop':
                    keep_alive = False
                    self.socket.send(b"Monitor stopped")
                    self.stop_server()
                else:
                    msg = json.loads(msg)
                    tocall = getattr(self, msg['action'])
                    tocall(*msg['args'])
                    self.socket.send(b"OK")
            except Exception, e:
                print e
                self.socket.send(b"Uh oh, something went wrong")
