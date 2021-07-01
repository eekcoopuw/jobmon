"""Sequential distributor that runs one task at a time."""
from collections import OrderedDict
import logging
import os
import shutil
from typing import Optional, List, Tuple, Dict, Any

from jobmon.cluster_type.base import ClusterDistributor, ClusterWorkerNode
from jobmon.constants import TaskInstanceStatus
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.worker_node.cli import WorkerNodeCLI


logger = logging.getLogger(__name__)


class LimitedSizeDict(OrderedDict):
    """Dictionary for exit info."""

    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key, value):
        """Set item in dict."""
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self):
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)


class SequentialDistributor(ClusterDistributor):
    """Executor to run tasks one at a time."""

    def __init__(self, exit_info_queue_size: int = 1000):
        """
        Args:
            exit_info_queue_size: how many exit codes to retain
        """
        self._next_distributor_id = 1
        self._exit_info = LimitedSizeDict(size_limit=exit_info_queue_size)

    @property
    def worker_node_entry_point(self):
        """Path to jobmon worker_node_entry_point"""
        return shutil.which("worker_node_entry_point")

    @property
    def cluster_type_name(self) -> str:
        return "sequential"

    def start(self) -> None:
        """Start the distributor."""
        raise NotImplementedError

    def stop(self, distributor_ids: List[int]) -> None:
        """Stop the distributor."""
        raise NotImplementedError

    def get_queueing_errors(self, distributor_ids: List[int]) -> Dict[int, str]:
        """Get the task instances that have errored out."""
        raise NotImplementedError

    def get_remote_exit_info(self, distributor_id: int) -> Tuple[str, str]:
        """Get exit info from task instances that have run."""
        try:
            exit_code = self._exit_info[distributor_id]
            if exit_code == 199:
                msg = "job was in kill self state"
                return TaskInstanceStatus.UNKNOWN_ERROR, msg
            else:
                return TaskInstanceStatus.UNKNOWN_ERROR, f"found {exit_code}"
        except KeyError:
            raise RemoteExitInfoNotAvailable

    def get_submitted_or_running(self, distributor_ids: List[int]) -> List[int]:
        """Check status of running task."""
        running = os.environ.get("JOB_ID")
        if running:
            return [int(running)]
        else:
            return []

    def terminate_task_instances(self, distributor_ids: List[int]) -> None:
        """If implemented, return a list of (task_instance_id, hostname) tuples for any
        task_instances that are terminated.
        """
        raise NotImplementedError

    def submit_to_batch_distributor(self, command: str, name: str,
                                    requested_resources: Dict[str, Any]) -> int:

        """Execute sequentially."""

        # add an executor id to the environment
        os.environ["JOB_ID"] = str(self._next_distributor_id)
        distributor_id = self._next_distributor_id
        self._next_distributor_id += 1

        # run the job and log the exit code
        try:
            cli = WorkerNodeCLI()
            args = cli.parse_args(command)
            exit_code = cli.run_task(args)
            #exit_code = unwrap(**parse_arguments(command))
        except SystemExit as e:
            if e.code == 199:
                exit_code = e.code
            else:
                raise

        self._exit_info[distributor_id] = exit_code
        return distributor_id


class SequentialWorkerNode(ClusterWorkerNode):
    """Get Executor Info for a Task Instance."""

    def __init__(self) -> None:
        self._distributor_id: Optional[int] = None

    @property
    def distributor_id(self) -> Optional[int]:
        """Distributor id of the task."""
        if self._distributor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._distributor_id = int(jid)
        return self._distributor_id

    def get_exit_info(self, exit_code: int, error_msg: str):
        """Exit info, error message."""
        return TaskInstanceStatus.ERROR, error_msg

    def get_usage_stats(self) -> Dict:
        """Usage information specific to the exector."""
        return {}

