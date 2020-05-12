from typing import Tuple, Union, Dict, Optional

from jobmon.client import ClientLogging as logging
from jobmon.client.execution.strategies.sge.sge_queue import SGE_ALL_Q, \
    SGE_GEOSPATIAL_Q, SGE_I_Q, SGE_LONG_Q

logger = logging.getLogger(__name__)


class SGEParameters:
    """Manages the SGE specific parameters requested for a given job, it will
    create an entry for what was requested if they are valid values
    It will then determine if that will run on the requested cluster and adjust
    accordingly
    """

    def __init__(self,
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
        self._cluster = "prod-el7"  # el7 in SGE_ENV is fair cluster
        self.num_cores = num_cores
        if isinstance(m_mem_free, str):
            m_mem_free = self._transform_mem_to_gb(m_mem_free)
            if isinstance(m_mem_free, str):
                logger.warning(f"This is not a memory measure that can be "
                               f"processed, setting to 1G")
                m_mem_free = 1
        self.m_mem_free = m_mem_free
        self.hard_limits = hard_limits
        if resource_scales is None:
            resource_scales = {'m_mem_free': 0.5, 'max_runtime_seconds': 0.5}
        self.resource_scales = resource_scales
        self.max_memory_gb = None
        self.min_memory_gb = None
        self.max_cores = None
        self.max_runtime = None

    @classmethod
    def set_executor_parameters_strategy(cls, executor_parameters):
        # first create an instance of the specific strategy
        instance = cls(
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

    def validation_msg(self) -> str:
        """
        If the object is valid, return an empty string, otherwise return an
        error message
        """
        q_msg, queue = self._validate_queue()
        self._set_max_limits(queue)
        cores_msg, _ = self._validate_num_cores()
        mem_msg, _ = self._validate_memory()
        runtime_msg, _ = self._validate_runtime()
        j_msg, _ = self._validate_j_resource()
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
        _, q = self._validate_queue()
        self.queue = q
        self._set_max_limits(q)
        _, cores = self._validate_num_cores()
        _, mem = self._validate_memory()
        _, j = self._validate_j_resource()
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
            logger.info("You have configured the resource adjustment value "
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
        if cores > self.max_cores:
            cores = self.max_cores
        self.num_cores = cores

    def _scale_m_mem_free(self):
        scale_by = self.resource_scales.get('m_mem_free')
        logger.debug(f"scaling mem by {scale_by}")
        mem = float(self.m_mem_free *
                    (1 + float(self.resource_scales.get('m_mem_free', 0))))
        if mem > self.max_memory_gb:
            mem = self.max_memory_gb
        self.m_mem_free = mem

    def _scale_max_runtime_seconds(self):
        scale_by = self.resource_scales.get('max_runtime_seconds')
        logger.debug(f"scaling max runtime by {scale_by}")
        runtime = int(self.max_runtime_seconds *
                      (1 + float(self.resource_scales.get('max_runtime_seconds', 0))))

        # check that new resources have not exceeded limits
        if runtime > self.max_runtime and 'all' in self.queue:
            if not self.hard_limits:
                logger.debug(
                    "runtime exceeded limits of all.q, moving to long.q")
                self.queue = 'long.q'
                self._set_max_limits('long.q')
                if runtime > self.max_runtime:
                    runtime = self.max_runtime
            else:
                runtime = SGE_ALL_Q.max_runtime_seconds

        self.max_runtime_seconds = runtime

    def _validate_num_cores(self) -> Tuple[str, int]:
        """Ensure cores requested isn't more than available on that
        node.
        """
        max_cores = self.max_cores
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
        if mem < self.min_memory_gb:
            mem = self.min_memory_gb
            return f"\n Memory: You requested below the minimum amount of " \
                   f"memory, set to {mem}", mem
        if mem > self.max_memory_gb:
            mem = self.max_memory_gb
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
        if self.max_runtime_seconds > SGE_LONG_Q.max_runtime_seconds and 'long' in self.queue:
            return f"Runtime exceeded maximum for long.q, setting to 16 days", \
                SGE_LONG_Q.max_runtime_seconds
        elif self.max_runtime_seconds > SGE_ALL_Q.max_runtime_seconds and self.hard_limits:
            return f"Runtime exceeded maximum for all.q and the user has set " \
                f"hard_limits to be enforced, therefore max_runtime is being " \
                f"capped at the all.q maximum", SGE_ALL_Q.max_runtime_seconds
        return "", self.max_runtime_seconds

    def _validate_j_resource(self) -> Tuple[str, bool]:
        if not(self.j_resource is True or self.j_resource is False):
            return f"\n J-Drive: j_resource is a bool arg. Got " \
                   f"{self.j_resource}", False
        return "", self.j_resource

    def _validate_queue(self) -> Tuple[str, Union[str, None]]:
        if self.max_runtime_seconds:
            if self.max_runtime_seconds > SGE_ALL_Q.max_runtime_seconds and not \
                    self.hard_limits:
                if self.queue is None or 'long' not in self.queue:
                    return f"\n Queue: queue provided has insufficient " \
                           f"max runtime, moved to long.q", 'long.q'
            elif self.queue is None and "el7" in self._cluster:
                return f"\n Queue: no queue was provided, setting to all.q", \
                       'all.q'

        elif self.queue is None and "el7" in self._cluster:
            return f"\n Queue: no queue was provided, setting to all.q", \
                   'all.q'
        return "", self.queue

    def _validate_resource_scales(self) -> Tuple[str, dict]:
        scales = self.resource_scales
        valid_resource_keys = ['m_mem_free',
                               'max_runtime_seconds', 'num_cores']
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

    def _set_max_limits(self, queue: str):
        if queue == "all.q":
            self.max_runtime = SGE_ALL_Q.max_runtime_seconds
            self.max_memory_gb = SGE_ALL_Q.max_memory_gb
            self.min_memory_gb = SGE_ALL_Q.min_memory_gb
            self.max_cores = SGE_ALL_Q.max_threads

        elif queue == "long.q":
            self.max_runtime = SGE_LONG_Q.max_runtime_seconds
            self.max_memory_gb = SGE_LONG_Q.max_memory_gb
            self.min_memory_gb = SGE_LONG_Q.min_memory_gb
            self.max_cores = SGE_ALL_Q.max_threads

        elif queue == "geospatial.q":
            self.max_runtime = SGE_GEOSPATIAL_Q.max_runtime_seconds
            self.max_memory_gb = SGE_GEOSPATIAL_Q.max_memory_gb
            self.min_memory_gb = SGE_GEOSPATIAL_Q.min_memory_gb
            self.max_cores = SGE_GEOSPATIAL_Q.max_threads

        elif queue == "i.q":
            self.max_runtime = SGE_I_Q.max_runtime_seconds
            self.max_memory_gb = SGE_I_Q.max_memory_gb
            self.min_memory_gb = SGE_I_Q.min_memory_gb
            self.max_cores = SGE_I_Q.max_threads

        else:
            logger.info("Unknown SGE queue name. Validating resources against "
                        "all.q standards")
            self.max_runtime = SGE_ALL_Q.max_runtime_seconds
            self.max_memory_gb = SGE_ALL_Q.max_memory_gb
            self.min_memory_gb = SGE_ALL_Q.min_memory_gb
            self.max_cores = SGE_ALL_Q.max_threads
