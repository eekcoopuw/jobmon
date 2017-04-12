from __future__ import print_function

import sys
import time
import multiprocessing

from jobmon.requester import Requester
from jobmon.models import Status
from jobmon.executors import base
from jobmon.exceptions import ReturnCodes
from jobmon import job

if sys.version_info > (3, 0):
    import subprocess
else:
    import subprocess32 as subprocess


# for sge logging of standard error
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class LocalJobInstance(job._AbstractJobInstance):
    """client node job status logger. Pushes job status to server node through
    zmq. Status is logged by server into sqlite database

    Args
        mon_dir (string): file path where the server configuration is
            stored.
        job_instance_id (int): unique id used to identify this job instance.
            Generally we use the process id.
        jid (int): job id that this job instance belongs to
        request_retries (int, optional): How many times to attempt to contact
            the central job monitor. Default=3
        request_timeout (int, optional): How long to wait for a response from
            the central job monitor. Default=3 seconds
    """

    def __init__(self, mon_dir, job_instance_id, jid, request_retries=3,
                 request_timeout=3000):
        """set SGE job id and job name as class attributes. discover from
        environment if not specified."""

        self.requester = Requester(mon_dir, request_retries, request_timeout)

        # get sge_id and name from envirnoment
        self.job_instance_id = job_instance_id
        self.jid = jid

        self.register_with_monitor()

    def log_job_stats(self):
        try:
            # TODO: would like to add move stat collecting perhaps using psutil
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

    def __init__(self, task_queue, task_response_queue, result_queue, mon_dir,
                 request_timeout, request_retries):
        """Consume work sent from LocalExecutor through multiprocessing queue.

        this class is structured based on
        https://pymotw.com/2/multiprocessing/communication.html

        Args:
            task_queue (multiprocessing.JoinableQueue): a JoinableQueue object
                created by LocalExecutor used to retrieve work from the
                executor
            task_response_queue (multiprocessing.Queue): a Queue to immediately
                respond to execute_async command with the pid of the newly
                spanwed subprocess.
            result_queue (multiprocessing.Queue): a Queue used to send updates
                on completed work to LocalExecutor class.
            mon_dir (str): filepath to instance of CentralJobMonitor
            request_timeout (int): how long will the requester wait at the
                socket for a response from CentralJobMonitor
            request_retries (int): how many times will the requester attempt to
                contact the CentralJobMonitor after a timeout waiting for a
                response
        """

        # this feels like the bad way but I copied it from the internets
        multiprocessing.Process.__init__(self)

        # consumer communication
        self.task_queue = task_queue
        self.task_response_queue = task_response_queue
        self.result_queue = result_queue
        self.daemon = True

        # resquester args
        self.mon_dir = mon_dir
        self.request_timeout = request_timeout
        self.request_retries = request_retries

    def run(self):
        """wait for work, the execute it"""
        while True:
            job_def = self.task_queue.get()
            if job_def is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break

            try:
                # open subprocess
                proc = subprocess.Popen(
                    ["python"] + [job_def.runfile] +
                    [str(arg) for arg in job_def.job_args],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)

                # start monitoring with subprocesses pid
                job_instance = LocalJobInstance(
                    mon_dir=self.mon_dir,
                    job_instance_id=proc.pid,
                    jid=job_def.jid,
                    request_timeout=self.request_timeout,
                    request_retries=self.request_retries)
                job_instance.log_started()
                self.task_response_queue.put(job_instance.job_instance_id)
            except ValueError as err:
                # according to POPEN docs a ValueError is raised if popen is
                # called with inproper arguments in which case LocalJobInstance
                # was never initialized
                proc.kill()
                eprint(err)
            else:
                stdout, stderr = proc.communicate(
                    timeout=job_def.process_timeout)
                print(stdout)
                eprint(stderr)

                # check return code
                if proc.returncode != ReturnCodes.OK:
                    job_instance.log_job_stats()
                    job_instance.log_error(str(stderr))
                    job_instance.log_failed()
                else:
                    job_instance.log_job_stats()
                    job_instance.log_completed()

            self.result_queue.put(job_instance.job_instance_id)
            self.task_queue.task_done()

            time.sleep(3)  # cycle for more work periodically


class PickledJob(object):

    def __init__(self, jid, runfile, job_args, process_timeout=None):
        """Internally used job representation to pass arguments to consumers"""
        self.jid = jid
        self.runfile = runfile
        self.job_args = job_args
        self.process_timeout = process_timeout


class LocalExecutor(base.BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.

    The subprocessing pattern looks like this.
        LocalExec
        --> consumer1
        ----> subconsumer1
        --> consumer2
        ----> subconsumer2
        ...
        --> consumerN
        ----> subconsumerN
    """

    def __init__(self, mon_dir, request_retries=3, request_timeout=3000,
                 parallelism=None, task_response_timeout=3):
        super(LocalExecutor, self).__init__(
            mon_dir, request_retries, request_timeout, parallelism)

        self.task_response_timeout = task_response_timeout

    def start(self):
        """fire up N task consuming processes using Multiprocessing. number of
        consumers is controlled by parallelism."""
        self.task_queue = multiprocessing.JoinableQueue()
        self.task_response_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.consumers = [
            LocalConsumer(
                task_queue=self.task_queue,
                task_response_queue=self.task_response_queue,
                result_queue=self.result_queue,
                mon_dir=self.mon_dir,
                request_timeout=self.request_timeout,
                request_retries=self.request_retries)
            for i in range(self.parallelism)
        ]

        for w in self.consumers:
            w.start()

    def execute_async(self, job, process_timeout=None):
        """add jobs to the actively processing queue.

        Args:
            job (jobmon.job.Job): instance of jobmon.job.Job
            process_timeout (int): time in seconds to wait for subprocess to
                finish. default is forever
        """
        job_def = PickledJob(job.jid, job.runfile, job.job_args,
                             process_timeout)
        self.task_queue.put(job_def)
        return self.task_response_queue.get(timeout=self.task_response_timeout)

    def _flush_unknown(self):
        """move things through the queue that finished with unknown status"""
        results = []
        while not self.result_queue.empty():
            job_instance_id = self.result_queue.get()
            jid = self._jid_from_job_instance_id(job_instance_id)
            results.append(jid)
        for jid in [j for j in self.running_jobs if j not in results]:
            self.jobs[jid]["status_id"] = Status.UNKNOWN

    def end(self):
        """terminate consumers and call sync 1 final time."""
        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # Wait for commands to finish
        self.task_queue.join()
        self._poll_status()
        self._flush_unknown()
