import multiprocessing
import os
import signal
import subprocess
from subprocess import TimeoutExpired
import sys
import time
import traceback


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
                    stderr=subprocess.PIPE,
                    preexec_fn=os.setsid)

                # start monitoring with subprocesses pid
                job_instance = LocalJobInstance(
                    mon_dir=self.mon_dir,
                    job_instance_id=proc.pid,
                    jid=job_def.jid,
                    request_timeout=self.request_timeout,
                    request_retries=self.request_retries)
                job_instance.log_started()
                self.task_response_queue.put(job_instance.job_instance_id)

                # communicate till done
                stdout, stderr = proc.communicate(
                    timeout=job_def.process_timeout)
                returncode = proc.returncode

            except TimeoutExpired:
                # kill process group
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                stdout, stderr = proc.communicate()
                stderr = stderr + " Process timed out after: {}".format(
                    job_def.process_timeout)
                returncode = proc.returncode

            except Exception as exc:
                stdout = ""
                stderr = "{}: {}\n{}".format(type(exc).__name__, exc,
                                             traceback.format_exc())
                returncode = None

            print(stdout)
            eprint(stderr)

            # check return code
            if returncode != ReturnCodes.OK:
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

    Args:
        mon_dir (string): directory where the connection info for the central
            job monitor is written
        request_retries (int, optional): how many time to try when pushing
            updates to the central job monitor
        request_timeout (int, optional): how long to linger on the zmq socket
            when pushing updates to the central job monitor and waiting for a
            response
        parallelism (int, optional): how many parallel jobs to schedule at a
            time
        subscribe_to_job_state (bool, optional): whether to subscribe to job
            state updates from the central job monitor via a zmq socket.
        task_response_timeout (int, optional): how long to wait for a consumer
            process to respond with the pid of the subprocess before giving up
    """

    def __init__(self,
                 parallelism=10, subscribe_to_job_state=True,
                 task_response_timeout=3):
        super(LocalExecutor, self).__init__(
            mon_dir, request_retries, request_timeout, parallelism,
            subscribe_to_job_state)

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

    def flush_lost_jobs(self):
        """move things through the queue that finished with unknown status"""
        results = []
        while not self.result_queue.empty():
            job_instance_id = self.result_queue.get()
            jid = self._jid_from_job_instance_id(job_instance_id)
            results.append(jid)
        finished_jobs = self.running_jobs + self.completed_jobs
        for jid in [j for j in results if j not in finished_jobs]:
            self.jobs[jid]["status_id"] = Status.UNREGISTERED_STATE

    def stop(self):
        """terminate consumers and call sync 1 final time."""
        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # Wait for commands to finish
        self.task_queue.join()
        self._poll_status()
        self.flush_lost_jobs()
