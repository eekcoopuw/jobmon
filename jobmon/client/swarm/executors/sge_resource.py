import os
import subprocess
import datetime
import logging
logger = logging.getLogger(__name__)


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
            run before the executor kills it. Not currently required by the
            new cluster, but will be. Default is None, for indefinite.
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
             If runtime isn't in the valid range for the associated queue
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

    def _get_valid_queues(self):
        check_valid_queues = "qconf -sql"
        valid_queues = subprocess.check_output(check_valid_queues,
                                               shell=True).split()
        return [q.decode("utf-8") for q in valid_queues]

    def _validate_queue(self):
        valid_queues = self._get_valid_queues()
        if self.queue is not None:
            valid = self.queue in valid_queues
            if not valid:
                logger.info(ValueError(f"Got invalid queue {self.queue}. "
                                       f"Valid queues are {valid_queues}"))
        if self.queue is None and "el7" in self._cluster:
            self.queue = "all.q"
        logger.debug("Now queues: {}, given queue: {}".format(valid_queues,
                                                              self.queue))

    def _validate_slots_and_cores(self):
        """Ensure cores requested isn't more than available on that
        node, at this point slots have been converted to cores
        """
        max_cores = 56
        if self.queue is not None:
            if 'c2' in self.queue:
                max_cores = 100
            elif self.queue == "geospatial.q":
                max_cores = 64
        if (self.num_cores is None or
            self.num_cores not in range(1, max_cores + 1)):
            raise ValueError("Got an invalid number of cores. Received {} "
                             "but the max_cores is {}"
                             .format(self.num_cores, max_cores))

    def _transform_mem_to_gb(self):
        # do we want upper and lowercase g, m, t options?
        mem = self.mem_free
        if mem[-1].lower() == "m":
            mem = float(self.mem_free[:-1])
            mem /= 1000
        elif mem[-2:].lower() == "mb":
            mem = float(mem[:-2])
            mem /= 1000
        elif mem[-1].lower() == "t":
            mem = float(mem[:-1])
            mem *= 1000
        elif mem[-2:].lower() == "tb":
            mem = float(mem[:-2])
            mem *= 1000
        elif mem[-1].lower() == "g":
            mem = float(mem[:-1])
        elif mem[-2:].lower() == "gb":
            mem = float(mem[:-2])
        else:
            raise ValueError("Memory measure should be an int followed by M, "
                             "MB, m, mb, G, GB, g, gb, T, TB, t, "
                             "or tb you gave {}".format(mem))
        return mem

    def _validate_memory(self):
        """Ensure memory requested isn't more than available on any node"""
        if self.mem_free is not None:
            self.mem_free = self._transform_mem_to_gb()
            if int(self.mem_free) not in range(0, 512):
                raise ValueError("Can only request mem_free_gb between "
                                 "0 and 512GB (the limit on all.q and "
                                 "profile.q). Got {}"
                                 .format(self.mem_free))
        else:
            logger.info("You have not specified a memory amount so you have "
                         "been given 1G, if you need more, add it is a "
                         "parameter to your Task")
            self.mem_free = 1

    def _transform_secs_to_hms(self):
        """UGE will accept seconds. Do not use funky formatting with days"""
        return str(self.max_runtime_seconds)

    def _validate_runtime(self):
        """Ensure that max_runtime passed in fits on the queue requested"""
        if self.max_runtime_seconds is None and "el7" in self._cluster:
            # a max_runtime has to be provided for the fair cluster, so if none
            #  is provided, set it to 5 minutes so that it fails quickly
            logger.info("You did not specify a maximum runtime so it has "
                         "been set to 5 minutes")
            self.max_runtime_seconds = 300
        elif self.max_runtime_seconds is not None:
            if self.queue == "all.q":
                if self.max_runtime_seconds > 3 * 86400:
                    raise ValueError("Can only run for up to 3 days"
                                     "(259200 sec)"
                                     " on all.q, you requested {} seconds"
                                     .format(self.max_runtime_seconds))
            elif self.queue == "geospatial.q":
                if self.max_runtime_seconds > 1555200:
                    raise ValueError("Can only run for up to 18 days "
                                     "(1555200 sec) on geospatial.q, you "
                                     "requested {} seconds"
                                     .format(self.max_runtime_seconds))
            elif self.queue == "long.q" or self.queue == "profile.q":
                if self.max_runtime_seconds > 604800:
                    raise ValueError("Can only run for up to 1 week "
                                     "(604800 sec) on {}, you requested {} "
                                     "seconds"
                                     .format(self.queue,
                                             self.max_runtime_seconds))
            elif self.max_runtime_seconds > 3 * 86400:
                raise ValueError("You did not request all.q, profile.q, "
                                 "geospatial.q, or long.q to run your jobs "
                                 "so you must limit your runtime to under 3 "
                                 "days")
        else:
            self.max_runtime_seconds = 0
        self.max_runtime = self._transform_secs_to_hms()

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
            for arg in [self.queue, self.num_cores, self.mem_free,
                        self.max_runtime_seconds]:
                if arg is None:
                    raise ValueError("To use {}, Your arguments for queue, "
                                     "num_cores/slots, mem_free, max_runtime"
                                     "_seconds can't be none, yours are:{} {} "
                                     "{} {}"
                                     .format(self._cluster, self.queue,
                                             self.num_cores, self.mem_free,
                                             self.max_runtime_seconds))
        # else: they have to have either slots or cores which is checked
        # in the _validate_exclusivity function

    def return_valid_resources(self):
        """Validate all resources and return them"""
        self._validate_exclusivity()
        self._validate_queue()
        self._validate_slots_and_cores()
        self._validate_memory()
        self._validate_runtime()
        self._validate_j_resource()
        self._validate_args_based_on_cluster()
        return (self.mem_free, self.num_cores, self.queue,
                self.max_runtime, self.j_resource)
