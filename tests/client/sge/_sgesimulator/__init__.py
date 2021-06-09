from jobmon.client.distributor.strategies.base import ExecutorParameters
from jobmon.client.distributor.strategies.sge.sge_executor import SGEExecutor, \
    TaskInstanceSGEInfo  # noqa F401
from jobmon.client.distributor.strategies.sge.sge_parameters import SGEParameters

ExecutorParameters.add_strategy(SGEParameters, '_SimulatorSGEExecutor')
