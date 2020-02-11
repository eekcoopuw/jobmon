from multiprocessing import JoinableQueue, Process, Queue
import os
import psutil
import subprocess
from typing import List, Optional, Dict, Tuple
import queue

from jobmon.client import ClientLogging as logging
from jobmon.client.execution.strategies.base import (Executor,
                                                     TaskInstanceExecutorInfo,
                                                     ExecutorParameters)
from jobmon.models.task_instance_status import TaskInstanceStatus

logger = logging.getLogger(__name__)


class Consumer(Process):

    def __init__(self, task_queue: JoinableQueue, response_queue: Queue):
        """Consume work sent from LocalExecutor through multiprocessing queue.

        this class is structured based on
        https://pymotw.com/2/multiprocessing/communication.html

        Args:
            task_queue (multiprocessing.JoinableQueue): a JoinableQueue object
                created by LocalExecutor used to retrieve work from the
                executor
        """

        # this feels like the bad way but I copied it from the internets
        super().__init__()

        # consumer communication
        self.task_queue = task_queue
        self.response_queue = response_queue

    def run(self):
        """wait for work, the execute it"""
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
                    os.environ["JOB_ID"] = str(task.executor_id)
                    logger.debug(f"consumer received {task.command}")
                    # run the job
                    proc = subprocess.Popen(
                        task.command,
                        env=os.environ.copy(),
                        shell=True)

                    # log the pid with the executor class
                    self.response_queue.put((task.executor_id, proc.pid))

                    # wait till the process finishes
                    proc.communicate()

                    # tell the queue this job is done so it can be shut down
                    # someday
                    self.response_queue.put((task.executor_id, None))
                    self.task_queue.task_done()

            except queue.Empty:
                pass


class PickableTask:
    """Object passed between processes"""

    def __init__(self, executor_id: int, command: str):
        self.executor_id = executor_id
        self.command = command


class MultiprocessExecutor(Executor):
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
        parallelism (int, optional): how many parallel jobs to schedule at a
            time
    """

    def __init__(self, parallelism: int = 10, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._parallelism = parallelism
        self._next_executor_id = 1

        # mapping of executor_id to pid. if pid is None then it is queued
        self._running_or_submitted: Dict[int, Optional[int]] = {}

        # ipc queues
        self.task_queue: JoinableQueue = JoinableQueue()
        self.response_queue: Queue = Queue()

    def start(self, jobmon_command=None) -> None:
        """fire up N task consuming processes using Multiprocessing. number of
        consumers is controlled by parallelism."""
        # set jobmon command if provided
        self.consumers = [
            Consumer(task_queue=self.task_queue,
                     response_queue=self.response_queue)
            for i in range(self._parallelism)
        ]
        for w in self.consumers:
            w.start()
        super().start(jobmon_command=jobmon_command)

    def stop(self) -> None:
        """terminate consumers and call sync 1 final time."""
        actual = self.get_actual_submitted_or_running()
        self.terminate_task_instances(actual)

        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # Wait for commands to finish
        self.task_queue.join()

    def _update_internal_states(self):
        while not self.response_queue.empty():
            executor_id, pid = self.response_queue.get()
            if pid is not None:
                self._running_or_submitted.update({executor_id: pid})
            else:
                self._running_or_submitted.pop(executor_id)

    def terminate_task_instances(self, executor_ids: List[int]) -> None:
        """Only terminate the task instances that are running, not going to
        kill the jobs that are actually still in a waiting or a transitioning
        state"""
        logger.debug(f"Going to terminate: {executor_ids}")

        # first drain the work queue so there are no race conditions with the
        # workers
        current_work = []
        work_order = {}
        i = 0
        while not self.task_queue.empty():
            current_work.append(self.task_queue.get())
            self.task_queue.task_done()
            # create a dictionary of the work indices for quick removal later
            work_order[i] = current_work[-1].executor_id
            i += 1

        # no need to worry about race conditions because there are no state
        # changes in the FSM caused by this method

        # now update our internal state tracker
        self._update_internal_states()

        # now terminate any running jobs and remove from state tracker
        for executor_id in executor_ids:
            # the job is running
            execution_pid = self._running_or_submitted.get(executor_id)
            if execution_pid is not None:
                # kill the process and remove it from the state tracker
                parent = psutil.Process(execution_pid)
                for child in parent.children(recursive=True):
                    child.kill()

            # a race condition. job finished before
            elif executor_id not in work_order.values():
                logger.error(
                    f"executor_id {executor_id} was requested to be terminated"
                    " but is not submitted or running")

        # if not running remove from queue and state tracker
        for index in sorted(work_order.keys(), reverse=True):
            if work_order[index] in executor_ids:
                del current_work[index]
                del self._running_or_submitted[work_order[index]]

        # put remaining work back on queue
        for task in current_work:
            self.task_queue.put(task)

    def get_actual_submitted_or_running(self) -> List[int]:
        self._update_internal_states()
        return list(self._running_or_submitted.keys())

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        executor_id = self._next_executor_id
        self._next_executor_id += 1
        task = PickableTask(executor_id, self.jobmon_command + " " + command)
        self.task_queue.put(task)
        self._running_or_submitted.update({executor_id: None})
        return executor_id


class TaskInstanceMultiprocessInfo(TaskInstanceExecutorInfo):

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._executor_id = int(jid)
        return self._executor_id

    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        msg = f"Got exit_code: {exit_code}. Error message was: {error_msg}"
        return TaskInstanceStatus.ERROR, msg
