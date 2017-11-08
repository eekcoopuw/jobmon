import pytest
from time import sleep

from jobmon import sge


def timeout_and_skip(step_size, max_time, max_qw, partial_test_function):
    """
    Utility function to wrap a test in a timeout loop. If it exceeds timeouts check if
    there were qwait states. If so, skip the test because it probably timed out due to cluster
    load.
    Args:
        step_size: number of seconds to wait between subsequent checks
        max_time: timeout in seconds
        max_qw: The max number of times that the test job is allowed to be in qw state, otherwise skip the test
        partial_test_function: Actually check the test if it is ready.
        returns true if it ran the test, false otherwise
    """
    total_sleep = 0
    qw_count = 0
    while True:
        sleep(step_size)
        total_sleep += step_size
        # There should now be a job that has errored out
        if partial_test_function():
            # The test passed, we are good
            break
        else:
            if total_sleep > max_time:
                if qw_count >= max_qw:
                    # The cluster is having a bad day
                    pytest.skip("Skipping test, saw too many ({}) qw states".format(max_qw))
                else:
                    print(sge.qstat())
                    assert False, \
                        "timed out (qwait count {}), might be:" \
                        "   a real bug," \
                        "   cluster load, or" \
                        "   project permissions for 'proj_qlogins'".format(qw_count)
            else:
                #  Probe qstat and count the number of qw states.
                #  If we aren't making much progress then dynamically skip the test.
                qstat_out = sge.qstat()
                job_row = qstat_out.loc[qstat_out['name'] == 'sleepyjob']
                if len(job_row) > 0:
                    # Make sure that job exists
                    job_status = job_row['status'].iloc[0]
                    if job_status == "qw":
                        qw_count += 1

    # To be clear that we are done and everything passed
    return True

