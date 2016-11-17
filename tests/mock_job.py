import os
import signal
import sys
import time

# TODO Should this be moved to a IHME testing utils repo? It is broadly useful


class MockJob(object):
    """Simulates a job. Takes some args, sleeps for a bit, and then either succeeds, throws an exception,
    or kills itself. Deliberately uses print, not logging"""

    DIE_COMMAND = "die job"
    """Use this string for the exception and the Job will call sys.exit"""

    def __init__(self, name, seconds_to_sleep, exception_to_raise):
        """
        Create a Mock Job.

        Args:
            :param name: Used for print statements to differentiate the different jobs
            :param seconds_to_sleep: The length of time it sleeps before dieing or succeeding
            :param exception_to_raise: If None or the null string then it succeeds. If IDE_COMMAND then
            it kills the python VM by calling sys.exit. If a non-empty string than it raises and Exception
            with that string.
        """
        self.name = name
        self.seconds_to_sleep = seconds_to_sleep
        self.exception_to_raise = exception_to_raise

    def run(self):
        """Execute the job."""
        print("Test Job {} starting, will sleep for {} and raise '{}'".format(self.name, self.seconds_to_sleep,
                                                                              self.exception_to_raise))
        time.sleep(int(self.seconds_to_sleep))
        if self.exception_to_raise is not None and self.exception_to_raise != "":
            if self.exception_to_raise == MockJob.DIE_COMMAND:
                # Die silently without trace
                # kill -9
                os.kill(os.getpid(), signal.SIGKILL)
                # I am now dead
            else:
                # Die "gracefully"
                print("raising exception '{}'".format(self.exception_to_raise))
                raise Exception(self.exception_to_raise)
        else:
            # Successful completion
            print("Mock job {} completed successfully".format(self.name))
            return True


if __name__ == "__main__":
    if len(sys.argv) == 4:
        job = MockJob(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print(
            "Mock_Job called with {} arguments, needs exactly 3 args"
            "(name, seconds to sleep, exception to raise (empty string means no-exception)".format(
                len(sys.argv)))
        sys.exit(-2)
    job.run()
