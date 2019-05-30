import os
import logging
from typing import Tuple, Union, Dict

from jobmon.client.swarm.executors.executor_parameters import ExecutorParameters
logger = logging.getLogger(__name__)

MIN_MEMORY_GB = 0.128
MAX_MEMORY_GB = 512

MAX_CORES = 56
MAX_CORES_GEOSPATIAL = 64
MAX_CORES_C2 = 100


class SGEParameters(ExecutorParameters):
    """Manages the SGE specific parameters requested for a given job, it will
    create an entry for what was requested if they are valid values
    It will then determine if that will run on the requested cluster and adjust accordingly"""

    def __init__(self, slots=None, mem_free=None, num_cores=None,
                 queue=None, max_runtime_seconds=None, j_resource=False,
                 m_mem_free=None):
        """
        Args
        slots (int): slots to request on the cluster
        mem_free (int): memory in gigabytes, in the old cluster syntax
        m_mem_free (str): amount of memory in gbs, tbs, or mbs to request on
            the cluster, this is deprecated and will be replaced with
            m_mem_free when the dev and prod clusters are taken offline.
        num_cores (int): number of cores to request on the cluster
        j_resource (bool): whether or not to access the J drive. Default: False
        queue (str): queue of cluster nodes to submit this task to. Must be
            a valid queue, as defined by "qconf -sql"
        max_runtime_seconds (int): how long the job should be allowed to
            run before the executor kills it. Currently required by the
            new cluster. Default is None, for indefinite.
        j_resource (bool): whether or not the job will need the J drive
        max_runtime: max_runtime_seconds converted into h:m:s style for new
        cluster

         Raises:
            ValueError:
             If not all required args are specified for the type of cluster
             If BOTH slots and cores are specified or neither are
             If the queue isn't a valid queue in qconf -sql
             If cores or slots aren't in a valid range: 1 to 48 or 100
             If mem_free is not in a valid range: 1GB to 1TB
             If m_mem_free is not in a valid range: 1GB to 1TB
             If mem_free and m_mem_free are both passed as arguments
             If max_runtime_seconds is not specified on the new cluster
             If j_resource isn't a bool

        Returns
            queue, slots, num_cores, mem_free_gb, max_runtime_secs
        """
        self.queue = queue
        self.max_runtime_seconds = max_runtime_seconds
        self.j_resource = j_resource
        self.max_runtime = None
        self._cluster = os.environ['SGE_ENV']  # el7 in SGE_ENV is fair cluster
        self.num_cores, self.m_mem_free = self._backward_compatible_resources(
            slots, num_cores, mem_free, m_mem_free)

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
        return num_cores, m_mem_free

    def is_valid(self) -> Tuple[bool, Dict, Union[str, None]]:
        """
        If the object is valid, return True and the validated set, otherwise
        (False, error_message), deliberately does not throw so it can be used
        where the client does not want an exception
        """
        cores_msg, cores = self._validate_num_cores()
        mem_msg, mem = self._validate_memory()
        runtime_msg, runtime = self._validate_runtime()
        j_msg, j = self._validate_j_resource()
        q_msg, queue = self._validate_queue()
        validated_params = {'cores': cores, 'mem': mem, 'runtime': runtime,
                            'queue': queue, 'j': j}
        if cores_msg or mem_msg or runtime_msg or j_msg or q_msg:
            return False, validated_params, \
                   f"You have one or more resource errors that was adjusted " \
                   f"for:\n Cores: {cores_msg}, \n Memory: {mem_msg},\n" \
                   f" Runtime: {runtime_msg}, \n J-Drive: {j_msg}, \n " \
                   f"Queue: {q_msg}"
        return True, validated_params, None

    @classmethod
    def return_validated(cls, validated_params) -> 'SGEParameters':
        return cls(num_cores=validated_params['cores'],
                   m_mem_free=validated_params['mem'],
                   max_runtime_seconds=validated_params['runtime'],
                   queue=validated_params['queue'],
                   j_resource=validated_params['j'])

    def return_adjusted(self, cores_adjustment: float=None,
                        mem_adjustment: float=None, runtime_adjustment:
                        float=None) -> 'SGEParameters':
        """
            If the parameters need to be adjusted then create and return a new
            object, otherwise None
        """
        super().return_adjusted()
        cores = self.num_cores
        mem = self.m_mem_free
        runtime = self.max_runtime_seconds
        if cores_adjustment:
            cores = self._adjust_num_cores(cores_adjustment)
        if mem_adjustment:
            mem = self._adjust_memory(mem_adjustment)
        if runtime_adjustment:
            runtime = self._adjust_runtime(runtime_adjustment)
        adjusted_params = {'cores': cores, 'mem': mem, 'runtime': runtime,
                           'queue': self.queue, 'j': self.j_resource}
        return adjusted_params

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

    def _adjust_num_cores(self, cores_adjustment: float) -> int:
        return int(self.num_cores+(self.num_cores*cores_adjustment))
        # validate that still in the allowed range


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
        return mem

    def _validate_memory(self) -> Tuple[str, float]:
        """Ensure memory requested isn't more than available on any node, and
         tranform it into a float in gigabyte form"""
        mem = self.m_mem_free
        if isinstance(mem, str):
            if mem[-1].lower() not in ["m", "g", "t"] and \
                    mem[-2:].lower() not in ["mb", "gb", "tb"]:
                return f"Memory measure should be an int followed by M, MB, m," \
                       f" mb, G, GB, g, gb, T, TB, t, or tb you gave " \
                       f"{mem}, setting to 1G", 1
            mem = self._transform_mem_to_gb(mem)
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

    def _adjust_memory(self, mem_adjustment: float) -> float:
        return self.m_mem_free + (self.m_mem_free * mem_adjustment)

    def _validate_runtime(self) -> Tuple[str, int]:
        """Ensure that max_runtime is specified for the new cluster"""
        new_cluster = "el7" in self._cluster
        if self.max_runtime_seconds is None and new_cluster:
            return "no runtime specified, setting to 24 hours", (24*60*60)
        elif isinstance(self.max_runtime_seconds, str):
            self.max_runtime_seconds = int(self.max_runtime_seconds)
        if self.max_runtime_seconds <= 0:
            return f"Max runtime must be strictly positive." \
                   f" Received {self.max_runtime_seconds}, " \
                   f"setting to 24 hours", (24*60*60)
        return "", self.max_runtime_seconds

    def _adjust_runtime(self, runtime_adjustment: float) -> float:
        return self.max_runtime_seconds + (self.max_runtime_seconds
                                           * runtime_adjustment)

    def _validate_j_resource(self) -> Tuple[str, bool]:
        if not(self.j_resource is True or self.j_resource is False):
            return f"j_resource is a bool arg. Got {self.j_resource}", False
        return "", True

    def _validate_queue(self) -> Tuple[str, Union[str, None]]:
        if self.queue is None and "el7" in self._cluster:
            return f"no queue was provided", 'all.q'
        return "", self.queue