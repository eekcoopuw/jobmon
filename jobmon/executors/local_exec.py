import os
import subprocess
import time
import multiprocessing

from jobmon.requester import Requester
from jobmon.models import Status
from jobmon.executors import base
from jobmon import job


# for sge logging of standard error
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class LocalJobInstance(job._AbstractJobInstance):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        mon_dir (string): file path where the server configuration is
            stored.
        jid (int, optional): job id to use when communicating with
            jobmon database. If job id is not specified, will register as a new
            job and aquire the job id from the central job monitor.

        *args and **kwargs are passed through to the responder
    """

    def __init__(self, mon_dir, jid=None, *args, **kwargs):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified."""

        self.requester = Requester(mon_dir, *args, **kwargs)

        # get sge_id and name from envirnoment
        self.job_instance_id = hash((os.getpid(), time.ctime()))
        self.jid = jid

        self.register_with_monitor()

    def log_job_stats(self):
        try:
            kwargs = {
                "wallclock": time.strftime("%H:%M:%S", time.localtime())}
            msg = {
                'action': 'update_job_instance_usage',
                'args': [self.job_instance_id],
                'kwargs': kwargs}
            self.requester.send_request(msg)
        except Exception as e:
            self.log_error(str(e))

    def register_with_monitor(self):
        """send registration request to server. server will create database
        entry for this job."""
        msg = {'action': 'register_job_instance',
               'kwargs': {'job_instance_id': self.job_instance_id,
                          'jid': self.jid,
                          'job_instance_type': self.job_instance_type}}
        r = self.requester.send_request(msg)
        return r[1]

    def log_started(self):
        """log job start with server"""
        msg = {'action': "update_job_instance_status",
               'args': [self.job_instance_id, Status.RUNNING]}
        self.requester.send_request(msg)

    def log_completed(self):
        """log job complete with server"""
        msg = {'action': "update_job_instance_status",
               'args': [self.job_instance_id, Status.COMPLETE]}
        self.requester.send_request(msg)

    def log_failed(self):
        """log job failure with server"""
        msg = {'action': "update_job_instance_status",
               'args': [self.job_instance_id, Status.FAILED]}
        self.requester.send_request(msg)

    def log_error(self, error_msg):
        """log job error with server"""
        msg = {
            'action': 'log_job_instance_error',
            'args': [self.job_instance_id, error_msg]}
        self.requester.send_request(msg)


class LocalConsumer(multiprocessing.Process):
    """this class is structured based on
       https://pymotw.com/2/multiprocessing/communication.html
    """

    def __init__(self, task_queue, result_queue, mon_dir, request_timeout,
                 request_retries):
        multiprocessing.Process.__init__(self)

        # consumer communication
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True

        # resquester args
        self.mon_dir = mon_dir
        self.request_timeout = request_timeout
        self.request_retries = request_retries

    def run(self):
        while True:
            job_def = self.task_queue.get()
            if job_def is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break

            # start monitoring
            job_instance = LocalJobInstance(
                mon_dir=self.mon_dir,
                jid=job_def.jid,
                request_timeout=self.request_timeout,
                request_retries=self.request_retries)
            job_instance.log_started()

            # open subprocess
            try:
                out = subprocess.check_output(
                    ["python"] + [job_def.runfile] +
                    [str(arg) for arg in job_def.job_args],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                eprint(exc.output)
                job_instance.log_job_stats()
                job_instance.log_error(str(exc.output))
                job_instance.log_failed()
            except:
                tb = traceback.format_exc()
                eprint(tb)
                job_instance.log_job_stats()
                job_instance.log_error(tb)
                job_instance.log_failed()
            else:
                print(out)
                job_instance.log_job_stats()
                job_instance.log_completed()

            self.result_queue.put(job_def.jid)
            self.task_queue.task_done()

            time.sleep(3)  # cycle for more work periodically


class PickledJob(object):

    def __init__(self, jid, runfile, job_args):
        self.jid = jid
        self.runfile = runfile
        self.job_args = job_args


class LocalExecutor(base.BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """

    def start(self):
        self.task_queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.consumers = [
            LocalConsumer(
                task_queue=self.task_queue,
                result_queue=self.result_queue,
                mon_dir=self.mon_dir,
                request_timeout=self.request_timeout,
                request_retries=self.request_retries)
            for i in range(self.parallelism)
        ]

        for w in self.consumers:
            w.start()

    def execute_async(self, job):
        job_def = PickledJob(job.jid, job.runfile, job.job_args)
        self.task_queue.put(job_def)
        return job_def.jid

    def sync(self):
        results = []
        while not self.result_queue.empty():
            results.append(self.result_queue.get())
        self.running_jobs = [j for j in self.running_jobs if j not in results]

    def end(self):
        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # Wait for commands to finish
        self.task_queue.join()
        self.sync()
