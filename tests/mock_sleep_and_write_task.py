import logging

import jobmon.client.swarm.workflow.executable_task as etk

logger = logging.getLogger(__name__)


class SleepAndWriteFileMockTask(etk.ExecutableTask):
    """
    A simple task that sleeps for a configured number of seconds, then writes a
    File. It can also be command to always fail (by raising ValueError), or
    fail a certain number of times and then succeed.

    The actual SGE command is remote_sleep_and_write.py
    """

    def __init__(self,
                 command,
                 upstream_tasks=None,
                 fail_always=False,
                 fail_count=0
                 ):
        etk.ExecutableTask.__init__(self, command, slots=1,
                                    upstream_tasks=upstream_tasks)
        # TBD validation using the types module.
        self.fail_always = fail_always
        self.fail_count = fail_count

        # NB this package must be installed into the conda env so that it will
        # be found
        self.output_file_name = command.split(
            '--output_file_path ')[1].split(" ")[0]
        self.command = command
        if self.fail_always:
            self.command += " --fail_always"
        if self.fail_count:
            self.command += " --fail_count {}".format(fail_count)
