import os
import logging
logger = logging.getLogger(__name__)

MIN_MEMORY_GB = 0.128
MAX_MEMORY_GB = 512

MAX_CORES = 56
MAX_CORES_GEOSPATIAL = 64
MAX_CORES_C2 = 100


class SGEResource(object):
    """Manages the transition between the new (cores/mem/runtime) cluster and
    old (slots). Can convert from new to old via the JSV, but not vice-versa
    (except for m_mem_free and mem_free). Validates inputs.
    """

    def __init__(self, slots=None, mem_free=None, num_cores=None,
                 queue=None, max_runtime_seconds=None, j_resource=False):
        """
        Args
        slots (int): slots to request on the cluster
        mem_free (str): amount of memory in gbs, tbs, or mbs to request on
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
        self.slots = slots
        self.num_cores = num_cores
        self.queue = queue
        self.max_runtime_seconds = max_runtime_seconds
        self.j_resource = j_resource
        self.max_runtime = None
        self.mem_free = mem_free
        self._cluster = os.environ['SGE_ENV']  # el7 in SGE_ENV is fair cluster

    def _validate_slots_and_cores(self):
        """Ensure cores requested isn't more than available on that
        node, at this point slots have been converted to cores
        """
        max_cores = MAX_CORES
        if self.queue is not None:
            if 'c2' in self.queue:
                max_cores = MAX_CORES_C2
            elif self.queue == "geospatial.q":
                max_cores = MAX_CORES_GEOSPATIAL
        if (self.num_cores is None or
            self.num_cores not in range(1, max_cores + 1)):
            raise ValueError("Got an invalid number of cores. Received {} "
                             "but the max_cores is {}"
                             .format(self.num_cores, max_cores))

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
            raise ValueError("Memory measure should be an int followed by M, "
                             "MB, m, mb, G, GB, g, gb, T, TB, t, "
                             "or tb you gave {}".format(mem_str))
        return mem

    def _validate_memory(self):
        """Ensure memory requested isn't more than available on any node"""
        if self.mem_free is not None:
            self.mem_free = self._transform_mem_to_gb(self.mem_free)
            if not (MIN_MEMORY_GB <= self.mem_free <= MAX_MEMORY_GB):
                raise ValueError(f"Can only request mem_free_gb between "
                                 f"{MIN_MEMORY_GB} and {MAX_MEMORY_GB}GB "
                                 f"profile.q). Received {self.mem_free}")
        else:
            logger.info("You have not specified a memory amount so you have "
                        "been given 1G, if you need more, add it is a "
                        "parameter to your Task")
            self.mem_free = 1

    def _transform_secs_to_hms(self):
        """UGE will accept seconds. Do not use funky formatting with days"""
        return

    def _validate_runtime(self):
        """Ensure that max_runtime is specified for the new cluster"""
        unspecified_runtime = self.max_runtime_seconds is None
        new_cluster = "el7" in self._cluster
        if unspecified_runtime and new_cluster:
            self.max_runtime_seconds = 24*60*60  # One day
            logger.info(
                f"max_runtime_seconds must be specified on the fair cluster,"
                f"no value given so setting to 24 hours")
        elif unspecified_runtime and not new_cluster:
            self.max_runtime_seconds = 1
        else:
            pass

        if self.max_runtime_seconds <= 0:
            raise ValueError(f"Max runtime must be strictly positive"
                             f"Received {self.max_runtime_seconds}")

        self.max_runtime = str(self.max_runtime_seconds)

    def _validate_j_resource(self):
        if not(self.j_resource is True or self.j_resource is False):
            raise ValueError("j_resource is a bool arg. Got {}"
                             .format(self.j_resource))

    def _validate_exclusivity(self):
        """Ensure there's no conflicting arguments, also if slots are
        specified, and num_cores are not, then set the num_cores to equal the
        slot value"""
        if self.slots and self.num_cores:
            raise ValueError("Cannot specify BOTH slots and num_cores. "
                             "Specify one or the other, your requested {} "
                             "slots and {} cores".format(self.slots,
                                                         self.num_cores))
        if not self.slots and not self.num_cores:
            raise ValueError("Must pass one of [slots, num_cores]")
        if self.slots and not self.num_cores:
            logger.info("User Specified slots instead of num_cores, so we are"
                        "converting it, but to run on the fair cluster they "
                        "should specify num_cores")
            self.num_cores = self.slots

    def _validate_args_based_on_cluster(self):
        """Ensure all essential arguments are present and not None, the fair
        cluster can be identified by its cluster name containing el7"""
        if "el7" in self._cluster:
            for arg in [self.num_cores, self.mem_free,
                        self.max_runtime_seconds]:
                if arg is None:
                    raise ValueError("To use {}, Your arguments for "
                                     "num_cores/slots, mem_free, max_runtime"
                                     "_seconds can't be none, yours are:{} "
                                     "{} {}"
                                     .format(self._cluster,
                                             self.num_cores, self.mem_free,
                                             self.max_runtime_seconds))
        # else: they have to have either slots or cores which is checked
        # in the _validate_exclusivity function

    def return_valid_resources(self):
        """Validate all resources and return them"""
        self._validate_exclusivity()
        self._validate_slots_and_cores()
        self._validate_memory()
        self._validate_runtime()
        self._validate_j_resource()
        self._validate_args_based_on_cluster()
        return (self.mem_free, self.num_cores, self.queue,
                self.max_runtime, self.j_resource)
