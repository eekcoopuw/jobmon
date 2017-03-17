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
        name (str, optional): name current process. If name is not specified
            will default to None.
        request_retries (int, optional): How many times to attempt to contact
            the central job monitor. Default=3
        request_timeout (int, optional): How long to wait for a response from
            the central job monitor. Default=3 seconds
    """

    def __init__(self, mon_dir, jid=None, name=None, runfile=None,
                 job_args=None, request_retries=3, request_timeout=3000):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified.
        """
        self.requester = Requester(mon_dir, request_retries, request_timeout)

        # get jid from monitor
        self.jid = jid
        self.name = name
        self.runfile = runfile

        self.job_args = job_args

        if self.jid is None:
            self.jid = self.register_with_monitor()

        self.job_instance_ids = []

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

        # Close out the zmq socket so that we don't run out of available
        # sockets when submitting lots of jobs via a single executor.
        # (Resource temporarily unavailable (src/thread.cpp:106))
        # We may want to make this behavior universal on the
        # requester.send_request() method
        self.requester.disconnect()
        return r[1]


# TODO: confirm this is working.
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
