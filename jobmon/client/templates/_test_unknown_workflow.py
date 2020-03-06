from jobmon.client.templates.unknown_workflow import UnknownWorkflow


class _TestUnknownWorkflow(UnknownWorkflow):
    def _set_executor(self, executor_class: str, *args, **kwargs) -> None:
        """Set the executor to the SGEExecutor simulator.

        Args:
            executor_class (str): string referring to one of the executor
            classes in jobmon.client.swarm.executors
        """
        self.executor_class = executor_class
        from jobmon.client.execution.strategies.sge.sge_executor import _SimulatorSGEExecutor
        self._executor = _SimulatorSGEExecutor(*args, **kwargs)
