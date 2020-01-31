from typing import Dict



class SGEQueue:
    """Each object in this class is the configuration of a queue on the
    cluster. It has the queue parameters that Jobmon is interested in.

    Used by SGEExecutor.

    For the initial release the values are hard-wired, from
    https://docs.cluster.ihme.washington.edu/allocation-and-limits/queues/

    When infra gives us an interface we will initialise this by probing the
    cluster when the Executor is created.
    """

    def __init__(self, name: str, max_threads: int, min_memory_gb: float,
                 max_memory_gb: float,
                 default_runtime_seconds: int, max_runtime_seconds: int):
        self.name = name
        self.max_threads = max_threads
        self.min_memory_gb = min_memory_gb
        self.max_memory_gb = max_memory_gb
        self.default_runtime_seconds = default_runtime_seconds
        self.max_runtime_seconds = max_runtime_seconds


ONE_DAY = 24 * 60 * 60

# The three queues, max memory is below node memory size per INFRA suggestion

SGE_ALL_Q = SGEQueue(name="all.q",
                     max_threads=56,
                     min_memory_gb=0.128, max_memory_gb=750,
                     default_runtime_seconds=ONE_DAY,
                     max_runtime_seconds=3 * ONE_DAY)

SGE_LONG_Q = SGEQueue(name="long.q",
                      max_threads=56,
                      min_memory_gb=0.128, max_memory_gb=750,
                      default_runtime_seconds=ONE_DAY,
                      max_runtime_seconds=16 * ONE_DAY)

SGE_GEOSPATIAL_Q = SGEQueue(name="geospatial.q",
                            max_threads=64,
                            min_memory_gb=0.128, max_memory_gb=1010,
                            default_runtime_seconds=ONE_DAY,
                            max_runtime_seconds=25 * ONE_DAY)

SGE_I_Q = SGEQueue(name="i.q",
                   max_threads=56,
                   min_memory_gb=0.128, max_memory_gb=750,
                   default_runtime_seconds=ONE_DAY,
                   max_runtime_seconds=7 * ONE_DAY)

# Look them up by name
queues_by_name: Dict[str, SGEQueue] = {
    SGE_ALL_Q.name: SGE_ALL_Q,
    SGE_LONG_Q.name: SGE_LONG_Q,
    SGE_GEOSPATIAL_Q.name: SGE_GEOSPATIAL_Q,
    SGE_I_Q.name: SGE_I_Q
}
