import logging

import jobmon.client.swarm.workflow.executable_task as etk

logger = logging.getLogger(__name__)


class MockLoadTestTask(etk.ExecutableTask):
    """
    A simple task that sleeps for a configured number of seconds, then writes a
    File. It can also be command to always fail (by raising ValueError), or
    fail a certain number of times and then succeed.

    The actual SGE command is sleep_and_error.py
    """

    def __init__(self,
                 command,
                 upstream_tasks=None,
                 fail_always=False,
                 fail_count=0,
                 sleep_timeout=False,
                 max_runtime_seconds=None
                 ):
        etk.ExecutableTask.__init__(self, command, num_cores=1, mem_free='2G',
                                    upstream_tasks=upstream_tasks,
                                    max_runtime_seconds=max_runtime_seconds)

        # TBD validation using the types module.
        self.fail_always = fail_always
        self.fail_count = fail_count
        self.sleep_timeout = sleep_timeout

        # NB this package must be installed into the conda env so that it will
        # be found
        self.command = command
        if self.fail_always:
            self.command += " --fail_always"
        if self.fail_count:
            self.command += " --fail_count {}".format(fail_count)
        if self.sleep_timeout:
            self.command += " --sleep_timeout"
