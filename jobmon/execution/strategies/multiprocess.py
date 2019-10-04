import atexit
import logging
from multiprocessing import JoinableQueue, Process, Queue
import os
import time
from typing import List, Optional

from jobmon.execution.strategies.base import (Executor,
                                              JobInstanceExecutorInfo,
                                              ExecutorParameters)
from jobmon.execution.worker_node.execution_wrapper import (unwrap,
                                                            parse_arguments)
from jobmon.models.job_instance_status import JobInstanceStatus

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
        while True:
            task = self.task_queue.get()
            if task is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break

            os.environ["JOB_ID"] = str(task.executor_id)

            # run the job
            unwrap(**parse_arguments(task.command))

            # tell the queue this job is done so it can be shut down someday
            self.task_queue.task_done()

            time.sleep(3)  # cycle for more work periodically


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
        self._running_or_submitted: set = set()

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
        # Sending poison pill to all worker
        for _ in self.consumers:
            self.task_queue.put(None)

        # TODO: we could force jobs to die if we get the processes of the
        # consumers

        # Wait for commands to finish
        self.task_queue.join()

    def get_actual_submitted_or_running(self) -> List[int]:
        while not self.response_queue.empty():
            self._running_or_submitted.remove(self.response_queue.get())
        return list(self._running_or_submitted)

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        executor_id = self._next_executor_id
        self._next_executor_id += 1
        task = PickableTask(executor_id, command)
        self.task_queue.put(task)
        return executor_id


class JobInstanceMultiprocessInfo(JobInstanceExecutorInfo):

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._executor_id = int(jid)
        return self._executor_id

    def get_exit_info(self, exit_code, error_msg):
        return JobInstanceStatus.ERROR, error_msg
