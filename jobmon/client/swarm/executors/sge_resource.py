import os
import subprocess
import datetime


class SGEResource(object):
    """Manages the transition between the new (cores)/mem/runtime) cluster and
    old (slots). Can convert from new to old via the JSV, but not vice-versa.
    Validates inputs.
    """

    def __init__(self, slots=None, mem_free_gb=None, num_cores=None,
                 queue=None, max_runtime_seconds=None, archive=False):
        """
        Args
        slots (int): slots to request on the cluster
        mem_free_gb (str): amount of memory in gbs, tbs, or mbs to request on
            the cluster
        num_cores (int): number of cores to request on the cluster.
        j_resource (bool): whether or not to access the J drive. Default: False
        queue (str): queue of cluster nodes to submit this task to. Must be
            a valid queue, as defined by "qconf -sql"
        max_runtime_secs (int): how long the job should be allowed to
            run before the executor kills it. Not currently required by the
            new cluster, but will be. Default is None, for indefinite.
        archive (bool): whether or not the job will need the J drive

         Raises:
            ValueError:
             If not all required args are specified for the type of cluster
             If BOTH slots and cores are specified or neither are
             If the queue isn't a valid queue in qconf -sql
             If cores or slots aren't in a valid range: 1 to 48 or 100
             If mem_free_gb is not in a valid range: 1GB to 1TB
             If runtime isn't in the valid range for the associated queue
             If archive isn't a bool

        Returns
            queue, slots, num_cores, mem_free_gb, max_runtime_secs
        """
        self.slots = slots
        self.mem_free_gb = mem_free_gb
        self.num_cores = num_cores
        self.archive = archive
        self.queue = queue
        self.max_runtime_seconds = max_runtime_seconds

    def _get_valid_queues(self):
        check_valid_queues = "qconf -sql"
        valid_queues = subprocess.check_output(check_valid_queues,
                                               shell=True).split()
        return [q.decode("utf-8") for q in valid_queues]

    def _validate_queue(self, queue):
        valid_queues = self._get_valid_queues()
        if queue:
            valid = any([q in queue for q in valid_queues])
            if not valid:
                raise ValueError("Got invalid queue {}. Valid queues are {}"
                                 .format(queue, valid_queues))

    def _validate_slots_and_cores(self):
        """Ensure slots or cores requested isn't more than available on that
        node
        """
        if 'c2' in self.queue:
            max_cores = 100
        else:
            max_cores = 56
        if (self.slots is not None and
            self.slots not in range(1, max_cores + 1)):
            raise ValueError("Got an invalid number of slots. Received {} "
                             "but the max_slots is {}"
                             .format(self.slots, max_cores))
        if (self.num_cores is not None and
            self.num_cores not in range(1, max_cores + 1)):
            raise ValueError("Got an invalid number of cores. Received {} "
                             "but the max_cores is {}"
                             .format(self.num_cores, max_cores))

    def _transform_mem_to_gb(self, mem):
        # do we want upper and lowercase g, m, t options?
        if mem[-1] == "M" or "m":
            mem = int(mem[:-1])
            mem /= 1000
        elif mem[-2:] == "MB" or "mb":
            mem = int(mem[:-2])
            mem /= 1000
        elif mem[-1] == "T" or "t":
            mem = int(mem[:-1])
            mem *= 1000
        elif mem[-2:] == "TB" or "tb":
            mem = int(mem[:-2])
            mem *= 1000
        elif mem[-1] == "G" or "g":
            mem = int(mem[:-1])
        elif mem[-2:] == "GB" or "gb":
            mem = int(mem[:-2])
        else:
            raise ValueError("Memory measure should be an int followed by M, "
                             "MB, m, mb, G, GB, g, gb, T, TB, t, "
                             "or tb you gave {]".format(mem))
        return mem

    def _validate_memory(self):
        """Ensure memory requested isn't more than available on any node"""
        self.mem_free_gb = self._transform_mem_to_gb(self.mem_free_gb)
        if self.mem_free_gb is not None:
            if self.mem_free_gb not in range(0, 512):
                raise ValueError("Can only request mem_free_gb between "
                                 "0 and 512GB (the limit on all.q and "
                                 "profile.q). Got {}"
                                 .format(self.mem_free_gb))

    def _transform_secs_to_hms(self, secs):
        return str(datetime.timedelta(seconds=secs))

    def _validate_runtime(self):
        """Ensure that max_runtime passed in fits on the queue requested"""
        if self.queue == "all.q":
            if self.max_runtime_seconds > 86400:
                raise ValueError("Can only run for up to 1 day (86400 sec) on "
                                 "all.q, you requested {} seconds"
                                 .format(self.max_runtime_seconds))
            else:
                self.max_runtime_seconds = self._transform_secs_to_hms(self.max_runtime_seconds)
        elif self.queue == "long.q" or self.queue == "profile.q":
            if self.max_runtime_seconds > 604800:
                raise ValueError("Can only run for up to 1 week (604800 sec) "
                                 "on {}, you requested {} seconds"
                                 .format(self.queue,
                                         self.max_runtime_seconds))
            else:
                self.max_runtime_seconds = self._transform_secs_to_hms(
                    self.max_runtime_seconds)
        else:
            raise ValueError("You did not request all.q, profile.q, or long.q "
                             "to run your jobs")

    def _validate_archive(self):
        if self.archive not in [True, False]:
            raise ValueError("archive is a bool arg. Got {}"
                             .format(self.archive))

    def _validate_exclusivity(self):
        """Ensure there's no conflicting arguments"""
        if self.slots and self.num_cores:
            raise ValueError("Cannot specify BOTH slots and num_cores. "
                             "Specify one or the other")
        if not self.slots and not self.num_cores:
            raise ValueError("Must pass one of [slots, num_cores]")

    def _validate_args_based_on_cluster(self):
        """Ensure all essential arguments are present and not None"""
        cluster = os.env['SGE_CLUSTER_NAMEf']
        if cluster == 'test_cluster':
            for arg in [self.queue, self.num_cores, self.mem_free_gb,
                        self.max_runtime_seconds]:
                if arg is None:
                    raise ValueError("To use {}, arg {} can't be None"
                                     .format(cluster, arg))
        # else: they have to have either slots or cores which is checked
        # in the _validate_exclusivity function

    def return_valid_resources(self):
        """Validate all resources and return them"""
        self._validate_args_based_on_cluster()
        self._validate_exclusivity()
        self._validate_queue()
        self._validate_slots_and_cores()
        self._validate_memory()
        self._validate_runtime()
        self._validate_j_resource()
        return (self.slots, self.mem_free_gb, self.num_cores, self.j_resource, self.queue,
                self.max_runtime_seconds)
