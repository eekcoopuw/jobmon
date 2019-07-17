import logging
import os
from typing import Tuple, Union, Dict, Optional

logger = logging.getLogger(__name__)

MIN_MEMORY_GB = 0.128
MAX_MEMORY_GB = 512

MAX_CORES = 56
MAX_CORES_GEOSPATIAL = 64
MAX_CORES_C2 = 100

MAX_RUNTIME_ALL = 259200
MAX_RUNTIME_LONG = 1382400


class SGEParameters:
    """Manages the SGE specific parameters requested for a given job, it will
    create an entry for what was requested if they are valid values
    It will then determine if that will run on the requested cluster and adjust
    accordingly
    """

    def __init__(self,
                 slots: Optional[int] = None,
                 mem_free: Optional[int] = None,
                 num_cores: Optional[int] = None,
                 queue: Optional[str] = None,
                 max_runtime_seconds: Optional[int] = None,
                 j_resource: bool = False,
                 m_mem_free: Optional[Union[str, float]] = None,
                 context_args: Union[Dict, str] = {},
                 hard_limits: Optional[bool] = False,
                 resource_scales: Dict = None):
        """
        Args:
            slots: slots to request on the cluster
            mem_free: memory in gigabytes, in the old cluster syntax
            m_mem_free: amount of memory in gbs, tbs, or mbs to
                request on the cluster, submitted from the user in string form
                but will be converted to a float for internal use
            num_cores: number of cores to request on the cluster
            j_resource: whether or not to access the J drive. Default: False
            queue: queue of cluster nodes to submit this task to. Must be
                a valid queue, as defined by "qconf -sql"
            max_runtime_seconds: how long the job should be allowed to
                run before the executor kills it. Currently required by the
                new cluster. Default is None, for indefinite.
            j_resource: whether or not the job will need the J drive
            context_args: additional arguments to be added for
                execution
            hard_limits: if the user wants jobs to stay on the chosen queue
                and not expand if resources are exceeded, set this to true
            resource_scales: for each resource, a scaling value can be provided
                so that different resources get scaled differently (scaling is
                only available on 'm_mem_free', 'num_cores' and
                'max_runtime_seconds')
        """
        self.queue = queue
        self.max_runtime_seconds = max_runtime_seconds
        self.j_resource = j_resource
        if context_args is None:
            context_args = {}
        self.context_args = context_args
        self._cluster = os.environ['SGE_ENV']  # el7 in SGE_ENV is fair cluster
        self.num_cores, self.m_mem_free = self._backward_compatible_resources(
            slots, num_cores, mem_free, m_mem_free)
        self.hard_limits = hard_limits
        if resource_scales is None:
            resource_scales = {'m_mem_free': 0.5, 'max_runtime_seconds': 0.5}
        self.resource_scales = resource_scales

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
            context_args=executor_parameters.context_args,
            hard_limits=executor_parameters.hard_limits,
            resource_scales=executor_parameters.resource_scales)

        # now set this instance as the underlying strategy and override the
        # underlying values for the post init values
        executor_parameters._strategy = instance
        executor_parameters._num_cores = instance.num_cores
        executor_parameters._queue = instance.queue
        executor_parameters._max_runtime_seconds = instance.max_runtime_seconds
        executor_parameters._j_resource = instance.j_resource
        executor_parameters._m_mem_free = instance.m_mem_free
        executor_parameters._context_args = instance.context_args
        executor_parameters._hard_limits = instance.hard_limits
        executor_parameters._resource_scales = instance.resource_scales

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
        scales_msg, _ = self._validate_resource_scales()
        if cores_msg or mem_msg or runtime_msg or j_msg or q_msg:
            msg = "You have one or more resource errors that was adjusted "
            for val in [cores_msg, mem_msg, runtime_msg, j_msg, q_msg]:
                if val:
                    msg += val
        else:
            msg = ""
        return msg

    def validate(self) -> None:
        _, cores = self._validate_num_cores()
        _, mem = self._validate_memory()
        _, j = self._validate_j_resource()
        _, q = self._validate_queue()
        self.queue = q
        # set queue so that runtime can be validated accordingly
        _, runtime = self._validate_runtime()
        _, resource_scales = self._validate_resource_scales()
        self.num_cores = cores
        self.m_mem_free = mem
        self.max_runtime_seconds = runtime
        self.j_resource = j
        self.resource_scales = resource_scales

    def adjust(self, **kwargs) -> None:
        """
            If only one specific parameter needs to be scaled, then scale that
            parameter, otherwise scale each parameter for which a scaling
            factor is available (default scale mem and runtime by 0.5)
        """
        only_scaled_resources = kwargs.get('only_scale', [])
        scale_all_by = kwargs.get('all_resource_scale_val', None)
        if scale_all_by is not None:
            logger.debug("You have configured the resource adjustment value "
                         "in your workflow, this will override any resource "
                         "specific scaling you have configured. If you would "
                         "like to scale your resources differently, configure "
                         "them only at the task level with the resource scales"
                         " parameter")
        for resource in only_scaled_resources:
            if scale_all_by is not None:
                self.resource_scales[resource] = scale_all_by
        if len(only_scaled_resources) > 0:
            if 'num_cores' in only_scaled_resources:
                self._scale_cores()
            if 'm_mem_free' in only_scaled_resources:
                self._scale_m_mem_free()
            if 'max_runtime_seconds' in only_scaled_resources:
                self._scale_max_runtime_seconds()
        else:
            self._scale_cores()
            self._scale_m_mem_free()
            self._scale_max_runtime_seconds()

    def _scale_cores(self):
        scale_by = self.resource_scales.get('num_cores', 0)
        logger.debug(f"scaling cores by {scale_by}")
        cores = int(self.num_cores *
                    (1 + float(self.resource_scales.get('num_cores', 0))))
        if cores > MAX_CORES:
            cores = MAX_CORES
        self.num_cores = cores

    def _scale_m_mem_free(self):
        scale_by = self.resource_scales.get('m_mem_free')
        logger.debug(f"scaling mem by {scale_by}")
        mem = float(self.m_mem_free * \
              (1 + float(self.resource_scales.get('m_mem_free', 0))))
        if mem > MAX_MEMORY_GB:
            mem = MAX_MEMORY_GB
        self.m_mem_free = mem

    def _scale_max_runtime_seconds(self):
        scale_by = self.resource_scales.get('max_runtime_seconds')
        logger.debug(f"scaling max runtime by {scale_by}")
        runtime = int(self.max_runtime_seconds *
                      (1 + float(self.resource_scales.get('max_runtime_seconds', 0))))

        # check that new resources have not exceeded limits
        if runtime > MAX_RUNTIME_ALL and 'all' in self.queue:
            if not self.hard_limits:
                logger.debug("runtime exceeded limits of all.q, moving to long.q")
                self.queue = 'long.q'
                if runtime > MAX_RUNTIME_LONG:
                    runtime = MAX_RUNTIME_LONG
            else:
                runtime = MAX_RUNTIME_ALL

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
            return "\n Cores: No cores specified, setting num_cores to 1", 1
        elif self.num_cores not in range(1, max_cores + 1):
            return f"\n Cores: Invalid number of cores. Got {self.num_cores}" \
                   f"cores but the max_cores is {max_cores}, setting cores to"\
                   f" 1", 1
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
            return f"\n Memory: You requested below the minimum amount of " \
                   f"memory, set to {mem}", mem
        if mem > MAX_MEMORY_GB:
            mem = MAX_MEMORY_GB
            return f"\n Memory: You requested above the maximum amount of " \
                   f" memory, set to the max allowed which is {mem}", mem
        return "", mem

    def _validate_runtime(self) -> Tuple[str, int]:
        """Ensure that max_runtime is specified for the new cluster"""
        new_cluster = "el7" in self._cluster
        if self.max_runtime_seconds is None and new_cluster:
            return ("\n Runtime: no runtime specified, setting to 24 hours",
                    (24 * 60 * 60))
        elif isinstance(self.max_runtime_seconds, str):
            self.max_runtime_seconds = int(self.max_runtime_seconds)
        if self.max_runtime_seconds <= 0:
            return f"\n Runtime: Max runtime must be strictly positive." \
                   f" Received {self.max_runtime_seconds}, " \
                   f"setting to 24 hours", (24 * 60 * 60)
        # make sure the time is below the cap if they provided a queue
        if self.max_runtime_seconds > MAX_RUNTIME_LONG and 'long' in self.queue:
            return f"Runtime exceeded maximum for long.q, setting to 16 days", \
                   MAX_RUNTIME_LONG
        elif self.max_runtime_seconds > MAX_RUNTIME_ALL and self.hard_limits:
            return f"Runtime exceeded maximum for all.q and the user has set " \
                f"hard_limits to be enforced, therefore max_runtime is being " \
                f"capped at the all.q maximum", MAX_RUNTIME_ALL
        return "", self.max_runtime_seconds

    def _validate_j_resource(self) -> Tuple[str, bool]:
        if not(self.j_resource is True or self.j_resource is False):
            return f"\n J-Drive: j_resource is a bool arg. Got " \
                   f"{self.j_resource}", False
        return "", self.j_resource

    def _validate_queue(self) -> Tuple[str, Union[str, None]]:
        if self.max_runtime_seconds:
            if self.max_runtime_seconds > MAX_RUNTIME_ALL and not \
                    self.hard_limits:
                if self.queue is None or 'long' not in self.queue:
                    return f"\n Queue: queue provided has insufficient " \
                           f"max runtime, moved to long.q", 'long.q'
        elif self.queue is None and "el7" in self._cluster:
            return f"\n Queue: no queue was provided, setting to all.q", \
                   'all.q'
        return "", self.queue

    def _validate_resource_scales(self) -> Tuple[str, dict]:
        scales = self.resource_scales
        valid_resource_keys = ['m_mem_free', 'max_runtime_seconds', 'num_cores']
        msg = ""
        if scales:
            for key in scales:
                if key not in valid_resource_keys:
                    msg = msg + f"{key} not a valid resource to scaled. Valid" \
                                f" options are: m_mem_free, max_runtime_seconds" \
                                f"and num_cores"
                elif scales.get(key) > 1 or scales.get(key) < 0:
                    scales[key] = 0.5
                    msg = msg + f"{key} was set to " \
                        f"{self.resource_scales.get(key)} which is above 1 or" \
                        f" below 0, it has been reset to 0.5 \n"
        else:
            scales = {}
        return msg, scales
