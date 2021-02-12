from jobmon.client.execution.strategies.base import ExecutorParameters
from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor, \
    TaskInstanceSGEInfo  # noqa F401
from jobmon.client.execution.strategies.sge.sge_parameters import SGEParameters

ExecutorParameters.add_strategy(SGEParameters, '_SimulatorSGEExecutor')
