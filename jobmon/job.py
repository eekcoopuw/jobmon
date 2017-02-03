import abc
from jobmon.requester import Requester


class Job(object):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        mon_dir (string): file path where the server configuration is
            stored.
        jid (int, optional): job id to use when communicating with
            jobmon database. If job id is not specified, will register as a new
            job and aquire the job id from the central job monitor.
        name (string, optional): name current process. If name is not specified
            will default to None.
    """

    def __init__(self, mon_dir, jid=None, name=None, runfile=None,
                 job_args=None, *args, **kwargs):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified.
        """
        self.requester = Requester(mon_dir, *args, **kwargs)

        # get jid from monitor
        self.jid = jid
        self.name = name
        self.runfile = runfile

        self.job_args = job_args

        if self.jid is None:
            self.jid = self.register_with_monitor()

        self.job_instances = []

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        job_args = [str(param) for param in self.job_args]
        job_args = ','.join(job_args) if job_args else None

        msg = {'action': 'register_job',
               'kwargs': {'name': self.name,
                          'runfile': self.runfile,
                          'job_args': job_args}}
        r = self.requester.send_request(msg)
        return r[1]


ABC = abc.ABCMeta('ABC', (object,), {})  # compatible with Python 2 *and* 3


class _AbstractJobInstance(ABC):
    """used to enforce a contract of available methods on job instance classes
    """

    @property
    def job_instance_type(self):
        return self.__class__.__name__

    @abc.abstractmethod
    def log_job_stats(self):
        return

    @abc.abstractmethod
    def register_with_monitor(self):
        return

    @abc.abstractmethod
    def log_started(self):
        return

    @abc.abstractmethod
    def log_completed(self):
        return

    @abc.abstractmethod
    def log_failed(self):
        return

    @abc.abstractmethod
    def log_error(self, error_msg):
        return
