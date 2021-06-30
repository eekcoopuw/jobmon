"""Multiprocess executes tasks in parallel if multiple threads are available."""
import logging
import os
import queue
import subprocess
from multiprocessing import JoinableQueue, Process, Queue
from typing import Dict, List, Optional, Tuple

#from jobmon.client.distributor.strategies.base import (Executor, ExecutorParameters,
#                                                       TaskInstanceExecutorInfo)
from jobmon.cluster_type.base import ClusterDistributor, ClusterWorkerNode
from jobmon.constants import TaskInstanceStatus

import psutil

logger = logging.getLogger(__name__)


class Consumer(Process):
    """Consumes the tasks to be run."""

    def __init__(self, task_queue: JoinableQueue, response_queue: Queue):
        """Consume work sent from LocalExecutor through multiprocessing queue.

        this class is structured based on
        https://pymotw.com/2/multiprocessing/communication.html

        Args:
            task_queue (multiprocessing.JoinableQueue): a JoinableQueue object
                created by LocalExecutor used to retrieve work from the
                distributor
        """
        super().__init__()

        # consumer communication
        self.task_queue = task_queue
        self.response_queue = response_queue

    def run(self) -> None:
        """Wait for work, the execute it."""
        logger.info(f"consumer alive. pid={os.getpid()}")

        while True:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:
                    logger.info("Received poison pill. Shutting down")
                    # Received poison pill, no more tasks to run
                    self.task_queue.task_done()
                    break

                else:
                    os.environ["JOB_ID"] = str(task.distributor_id)
                    logger.debug(f"consumer received {task.command}")
                    # run the job
                    proc = subprocess.Popen(
                        task.command,
                        env=os.environ.copy(),
                        shell=True
                    )

                    # log the pid with the distributor class
                    self.response_queue.put((task.distributor_id, proc.pid))

                    # wait till the process finishes
                    proc.communicate()

                    # tell the queue this job is done so it can be shut down
                    # someday
                    self.response_queue.put((task.distributor_id, None))
                    self.task_queue.task_done()

            except queue.Empty:
                pass


class PickableTask:
    """Object passed between processes."""

    def __init__(self, distributor_id: int, command: str):
        self.distributor_id = distributor_id
        self.command = command


class MultiprocessDistributor(ClusterDistributor):
    """ executes tasks locally in parallel. It uses the multiprocessing Python
    library and queues to parallelize the execution of tasks.

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
        parallelism (int, optional): how many parallel jobs to distribute at a
            time
    """

    def __init__(self, parallelism: int = 10, *args, **kwargs) -> None:
        self.temp_dir: Optional[str] = None
        self.started = False
        #self._jobmon_command = shutil.which(("jobmon_command")) #worker_node_wrapper_executable
        self.worker_node_entry_point = shutil.which(("worker_node_entry_point"))  #
        logger.info("Initializing {}".format(self.__class__.__name__))

        self._parallelism = parallelism
        self._next_distributor_id = 1

        # mapping of distributor_id to pid. if pid is None then it is queued
        self._running_or_submitted: Dict[int, Optional[int]] = {}

        # ipc queues
        self.task_queue: JoinableQueue = JoinableQueue()
        self.response_queue: Queue = Queue()

        # workers
        self.consumers: List[Consumer] = []


    @property
    def cluster_type_name(self) -> str:
        return "multiprocess"


    def start(self) -> None:
        """Fire up N task consuming processes using Multiprocessing. number of consumers is
        controlled by parallelism.
        """
        # set jobmon command if provided
        self.consumers = [
            Consumer(task_queue=self.task_queue,
                     response_queue=self.response_queue)
            for i in range(self._parallelism)
        ]
        for w in self.consumers:
            w.start()

        """Start the default."""
        self.started = True

    def stop(self, distributor_ids: List[int]) -> None:
        """Terminate consumers and call sync 1 final time."""
        actual = self.get_actual_submitted_or_running(distributor_ids=distributor_ids)
        self.terminate_task_instances(actual)

        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # Wait for commands to finish
        self.task_queue.join()

        """Stop the default."""
        self.started = False

    def _update_internal_states(self) -> None:
        while not self.response_queue.empty():
            distributor_id, pid = self.response_queue.get()
            if pid is not None:
                self._running_or_submitted.update({distributor_id: pid})
            else:
                self._running_or_submitted.pop(distributor_id)

    def terminate_task_instances(self, distributor_ids: List[int]) -> None:
        """Only terminate the task instances that are running, not going to kill the jobs that
        are actually still in a waiting or a transitioning state.
        """
        logger.debug(f"Going to terminate: {distributor_ids}")

        # first drain the work queue so there are no race conditions with the
        # workers
        current_work = []
        work_order = {}
        i = 0
        while not self.task_queue.empty():
            current_work.append(self.task_queue.get())
            self.task_queue.task_done()
            # create a dictionary of the work indices for quick removal later
            work_order[i] = current_work[-1].distributor_id
            i += 1

        # no need to worry about race conditions because there are no state
        # changes in the FSM caused by this method

        # now update our internal state tracker
        self._update_internal_states()

        # now terminate any running jobs and remove from state tracker
        for distributor_id in distributor_ids:
            # the job is running
            execution_pid = self._running_or_submitted.get(distributor_id)
            if execution_pid is not None:
                # kill the process and remove it from the state tracker
                parent = psutil.Process(execution_pid)
                for child in parent.children(recursive=True):
                    child.kill()

            # a race condition. job finished before
            elif distributor_id not in work_order.values():
                logger.error(
                    f"distributor_id {distributor_id} was requested to be terminated"
                    " but is not submitted or running")

        # if not running remove from queue and state tracker
        for index in sorted(work_order.keys(), reverse=True):
            if work_order[index] in distributor_ids:
                del current_work[index]
                del self._running_or_submitted[work_order[index]]

        # put remaining work back on queue
        for task in current_work:
            self.task_queue.put(task)

    def get_actual_submitted_or_running(self, distributor_ids: List[int]) -> List[int]:
        """Get tasks that are active."""
        self._update_internal_states()
        return list(self._running_or_submitted.keys())

    def execute(self, command: str, name: str, requested_resources: dict) -> int:
        """Execute a task instance."""
        distributor_id = self._next_distributor_id
        self._next_distributor_id += 1
        # task = PickableTask(distributor_id, self.jobmon_command + " " + command)
        task = PickableTask(distributor_id, command)
        self.task_queue.put(task)
        self._running_or_submitted.update({distributor_id: None})
        return distributor_id


    def worker_node_wrapper_executable(self):
        """Path to jobmon worker node executable"""
        return self.worker_node_entry_point


    def get_queueing_errors(self, distributor_ids: List[int]) -> Dict[int, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError


    def get_remote_exit_info(self, distributor_id: int) -> Tuple[str, str]:
        """Get the exit info about the task instance once it is done running."""
        raise RemoteExitInfoNotAvailable


class MultiprocessWorkerNode(ClusterWorkerNode):
    """Task instance info for an instance run with the Multiprocessing distributor."""

    def __init__(self) -> None:
        self._distributor_id: Optional[int] = None

    @property
    def distributor_id(self) -> Optional[int]:
        """The id from the distributor."""
        if self._distributor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._distributor_id = int(jid)
        return self._distributor_id

    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        """Exit code and message."""
        msg = f"Got exit_code: {exit_code}. Error message was: {error_msg}"
        return TaskInstanceStatus.ERROR, msg


    def get_usage_stats(self) -> Dict:
        """Usage information specific to the distributor."""
        raise NotImplementedError
