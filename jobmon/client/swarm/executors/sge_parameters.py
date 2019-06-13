import logging
import os
from typing import Tuple, Union, Dict, Optional

logger = logging.getLogger(__name__)

MIN_MEMORY_GB = 0.128
MAX_MEMORY_GB = 512

MAX_CORES = 56
MAX_CORES_GEOSPATIAL = 64
MAX_CORES_C2 = 100


class SGEParameters:
    """Manages the SGE specific parameters requested for a given job, it will
    create an entry for what was requested if they are valid values
    It will then determine if that will run on the requested cluster and adjust
    accordingly

    Args
        slots (int): slots to request on the cluster
        mem_free (int): memory in gigabytes, in the old cluster syntax
        m_mem_free (str or float): amount of memory in gbs, tbs, or mbs to
            request on the cluster, submitted from the user in string form but
            will be converted to a float for internal use
        num_cores (int): number of cores to request on the cluster
        j_resource (bool): whether or not to access the J drive. Default: False
        queue (str): queue of cluster nodes to submit this task to. Must be
            a valid queue, as defined by "qconf -sql"
        max_runtime_seconds (int): how long the job should be allowed to
            run before the executor kills it. Currently required by the
            new cluster. Default is None, for indefinite.
        j_resource (bool): whether or not the job will need the J drive
            context_args (dict or str): additional arguments to be added for
            execution
    """

    def __init__(self,
                 slots: Optional[int] = None,
                 mem_free: Optional[int] = None,
                 num_cores: Optional[int] = None,
                 queue: Optional[str] = None,
                 max_runtime_seconds: Optional[int] = None,
                 j_resource: bool = False,
                 m_mem_free: Optional[Union[str, float]] = None,
                 context_args: Optional[Union[Dict, str]] = None):

        self.queue = queue
        self.max_runtime_seconds = max_runtime_seconds
        self.j_resource = j_resource
        self.context_args = context_args
        self._cluster = os.environ['SGE_ENV']  # el7 in SGE_ENV is fair cluster
        self.num_cores, self.m_mem_free = self._backward_compatible_resources(
            slots, num_cores, mem_free, m_mem_free)

    @classmethod
    def set_executor_parameters_strategy(cls, executor_parameters):
        # first create an instance of the specific strategy
        instance = cls(
            slots=executor_parameters._slots,
            mem_free=executor_parameters._mem_free,
            num_cores=executor_parameters.num_cores,
            queue=executor_parameters.queue,
            max_runtime_seconds=executor_parameters.max_runtime_seconds,
            j_resource=executor_parameters.j_resource,
            m_mem_free=executor_parameters.m_mem_free,
            context_args=executor_parameters.context_args)

        # now set this instance as the underlying strategy and override the
        # underlying values for the post init values
        executor_parameters._strategy = instance
        executor_parameters._num_cores = instance.num_cores
        executor_parameters._queue = instance.queue
        executor_parameters._max_runtime_seconds = instance.max_runtime_seconds
        executor_parameters._j_resource = instance.j_resource
        executor_parameters._m_mem_free = instance.m_mem_free
        executor_parameters._context_args = instance.context_args

    def _backward_compatible_resources(self, slots, num_cores, mem_free,
                                       m_mem_free):
        """
        Ensure there are no conflicting or missing arguments for memory and
        cores since we are allowing old cluster syntax and new cluster syntax
        """
        if slots and num_cores:
            logger.warning("Cannot specify BOTH slots and num_cores. Using "
                           "the num_cores you specified to set the value")
        elif not slots and not num_cores:
            logger.warning("Must pass one of [slots, num_cores], will set to "
                           "default 1 core for this run")
        elif slots and not num_cores:
            logger.info("User Specified slots instead of num_cores, so we are"
                        "converting it, but to run on the fair cluster they "
                        "should specify num_cores")
            num_cores = slots
        if mem_free and m_mem_free:
            logger.warning(f"Cannot specify BOTH mem_free and m_mem_free. "
                           f"Specify one or the other, you requested "
                           f"{mem_free}G mem_free and {m_mem_free} "
                           f"m_mem_free, defaults to m_mem_free value")
        elif mem_free and not m_mem_free:
            m_mem_free = mem_free
        elif not mem_free and not m_mem_free:
            logger.warning("Must pass one of [mem_free, m_mem_free], will set "
                           "to default 1G for this run")
        if isinstance(m_mem_free, str):
            m_mem_free = self._transform_mem_to_gb(m_mem_free)
            if isinstance(m_mem_free, str):
                logger.warning(f"This is not a memory measure that can be "
                               f"processed, setting to 1G")
                m_mem_free = 1
        return num_cores, m_mem_free

    def validation_msg(self) -> str:
        """
        If the object is valid, return an empty string, otherwise return an
        error message
        """
        cores_msg, _ = self._validate_num_cores()
        mem_msg, _ = self._validate_memory()
        runtime_msg, _ = self._validate_runtime()
        j_msg, _ = self._validate_j_resource()
        q_msg, _ = self._validate_queue()
        msg = ""
        if cores_msg or mem_msg or runtime_msg or j_msg or q_msg:
            msg = f"You have one or more resource errors that was adjusted " \
                f"for:\n Cores: {cores_msg}, \n Memory: {mem_msg},\n" \
                f" Runtime: {runtime_msg}, \n J-Drive: {j_msg}, \n " \
                f"Queue: {q_msg}"
        return msg

    def validate(self) -> None:
        _, cores = self._validate_num_cores()
        _, mem = self._validate_memory()
        _, runtime = self._validate_runtime()
        _, j = self._validate_j_resource()
        _, q = self._validate_queue()
        self.num_cores = cores
        self.m_mem_free = mem
        self.max_runtime_seconds = runtime
        self.j_resource = j
        self.queue = q

    def adjust(self, **kwargs) -> None:
        """
            If the parameters need to be adjusted then create and return a new
            object, otherwise None
        """
        cores = int(self.num_cores * (1 + float(kwargs.get('num_cores', 0))))
        mem = self.m_mem_free * (1 + float(kwargs.get('m_mem_free', 0)))
        runtime = int(self.max_runtime_seconds *
                      (1 + float(kwargs.get('max_runtime_seconds', 0))))
        self.num_cores = cores
        self.m_mem_free = mem
        self.max_runtime_seconds = runtime

    def _validate_num_cores(self) -> Tuple[str, int]:
        """Ensure cores requested isn't more than available on that
        node, at this point slots have been converted to cores
        """
        max_cores = MAX_CORES
        if self.queue is not None:
            if 'c2' in self.queue:
                max_cores = MAX_CORES_C2
            elif self.queue == "geospatial.q":
                max_cores = MAX_CORES_GEOSPATIAL
        if self.num_cores is None:
            return "No cores specified, setting num_cores to 1", 1
        elif self.num_cores not in range(1, max_cores + 1):
            return f"Invalid number of cores. Got {self.num_cores} cores" \
                   f" but the max_cores is {max_cores}, setting cores to 1", 1
        return "", self.num_cores

    @staticmethod
    def _transform_mem_to_gb(mem_str: str) -> float:
        # we allow both upper and lowercase g, m, t options
        # BUG g and G are not the same
        if mem_str[-1].lower() == "m":
            mem = float(mem_str[:-1])
            mem /= 1000
        elif mem_str[-2:].lower() == "mb":
            mem = float(mem_str[:-2])
            mem /= 1000
        elif mem_str[-1].lower() == "t":
            mem = float(mem_str[:-1])
            mem *= 1000
        elif mem_str[-2:].lower() == "tb":
            mem = float(mem_str[:-2])
            mem *= 1000
        elif mem_str[-1].lower() == "g":
            mem = float(mem_str[:-1])
        elif mem_str[-2:].lower() == "gb":
            mem = float(mem_str[:-2])
        else:
            mem = 1
        return mem

    def _validate_memory(self) -> Tuple[str, float]:
        """Ensure memory requested isn't more than available on any node, and
         tranform it into a float in gigabyte form"""
        mem = self.m_mem_free
        if mem is None:
            mem = 1  # set to 1G
        if mem < MIN_MEMORY_GB:
            mem = MIN_MEMORY_GB
            return f"You requested below the minimum amount of memory, set " \
                   f"to {mem}", mem
        if mem > MAX_MEMORY_GB:
            mem = MAX_MEMORY_GB
            return f"You requested above the maximum amount of memory, set " \
                   f"to the max allowed which is {mem}", mem
        return "", mem

    def _validate_runtime(self) -> Tuple[str, int]:
        """Ensure that max_runtime is specified for the new cluster"""
        new_cluster = "el7" in self._cluster
        if self.max_runtime_seconds is None and new_cluster:
            return "no runtime specified, setting to 24 hours", (24 * 60 * 60)
        elif isinstance(self.max_runtime_seconds, str):
            self.max_runtime_seconds = int(self.max_runtime_seconds)
        if self.max_runtime_seconds <= 0:
            return f"Max runtime must be strictly positive." \
                   f" Received {self.max_runtime_seconds}, " \
                   f"setting to 24 hours", (24 * 60 * 60)
        return "", self.max_runtime_seconds

    def _validate_j_resource(self) -> Tuple[str, bool]:
        if not(self.j_resource is True or self.j_resource is False):
            return f"j_resource is a bool arg. Got {self.j_resource}", False
        return "", self.j_resource

    def _validate_queue(self) -> Tuple[str, Union[str, None]]:
        if self.queue is None and "el7" in self._cluster:
            return f"no queue was provided, setting to all.q", 'all.q'
        return "", self.queue
