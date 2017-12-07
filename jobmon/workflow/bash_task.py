import logging

from jobmon.workflow.executable_task import ExecutableTask

logger = logging.getLogger(__name__)


class BashTask(ExecutableTask):

    def __init__(self, command, upstream_tasks=[]):
        ExecutableTask.__init__(
            self, command, upstream_tasks=upstream_tasks)
        self.command = command
