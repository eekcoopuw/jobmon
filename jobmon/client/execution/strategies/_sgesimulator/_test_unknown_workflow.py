from jobmon.client.templates.unknown_workflow import UnknownWorkflow
from jobmon.client.execution.strategies._sgesimulator._sgesimulator import _SimulatorSGEExecutor


class _TestUnknownWorkflow(UnknownWorkflow):
    def _set_executor(self, executor_class: str, *args, **kwargs) -> None:
        """Set the executor to the SGEExecutor simulator.

        Args:
            executor_class (str): string referring to one of the executor
            classes in jobmon.client.swarm.executors
        """
        self.executor_class = executor_class
        self._executor = _SimulatorSGEExecutor(*args, **kwargs)
