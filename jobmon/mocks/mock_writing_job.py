import os
import sys

from jobmon.mocks.mock_job import MockJob


class MockWritingJob(MockJob):
    """Simulates a job. Takes some args, sleeps for a bit, and then either succeeds, throws an exception,
    or kills itself. Deliberately uses print, not logging"""

    MOCK_DATA = "Mock contents for testing\n"

    def __init__(self, name, seconds_to_sleep, exception_to_raise, files_to_write, content=MOCK_DATA, logger=None):
        """
        Create a MockWritingJob, with a set of files to write if it succeeds.

        Args:
            :param name: Used for print statements to differentiate the different jobs
            :param seconds_to_sleep: The length of time it sleeps before dieing or succeeding
            :param exception_to_raise: If None or the null string then it succeeds. If DIE_COMMAND then
            it kills the python VM by calling sys.exit. If a non-empty string than it raises and Exception
            with that string.
            :param files_to_write list of full file paths, will write a dummy message to these files on success
        """
        MockJob.__init__(self, name, seconds_to_sleep, exception_to_raise)
        self.files_to_write = files_to_write if files_to_write else []
        self.content = content
        self.logger = logger

    def action_succeed(self):
        """Write my files, deliberately let any IO errors be thrown."""
        for filename in self.files_to_write:
            if self.logger:
                self.logger.info("Writing mock file {}".format(filename))
            dir = os.path.dirname(filename)
            if not os.path.exists(dir):
                try:
                    if self.logger:
                        self.logger.info("creating dir {}".format(dir))
                    os.makedirs(dir)
                except:
                    pass
            f = open(filename, 'w+')
            f.write(self.content)
            f.close()


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        job = MockWritingJob(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:])
    else:
        print(
            "MockWritingJob called with {} arguments, needs at least 4 args"
            "(name, seconds to sleep, exception to raise (empty string means no-exception),"
            " one or more files to write".format(
                len(sys.argv)))
        sys.exit(2)
    job.run()
