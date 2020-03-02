from jobmon.client.execution.strategies.sge.sge_executor import (
    SGEExecutor, TaskInstanceSGEInfo)
from jobmon.client.execution.strategies.sge.sge_parameters import SGEParameters

from jobmon.client.execution.strategies.base import ExecutorParameters

ExecutorParameters.add_strategy(SGEParameters, 'SGEExecutor')
