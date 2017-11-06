import pytest

from time import sleep

from jobmon import sge


def timeout_and_skip(initial_sleep, step_size, max_time, max_qw, test_partial_function):
    """
    Utility function to wrap a test ina timeout loop. If it exceeds timeouts check if
    there were qwait states. If so, skip the test because it probably timed out due to cluster
    load.
    Args:
        initial_sleep: Sleep this number of seconds before testing once
        step_size: number of seconds to wait between subsequent checks
        max_time: timeout in seconds
        max_qw: If this  number ofr more qw states are seen in the test job then skip
        test_partial_function: Actually check the test if it is ready.
        returns true if it ran the test, false otherwise
    """
    sleep(initial_sleep)
    total_sleep = initial_sleep
    qw_count = 0
    while True:
        sleep(step_size)
        total_sleep += step_size
        # There should now be a job that has errored out
        if test_partial_function():
            # The test passed
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
                # Wow, pandas can be hard to use at times
                job_row = qstat_out.loc[qstat_out['name'] == 'sleepyjob']
                if len(job_row) > 0:
                    job_status = job_row['status'].iloc[0]
                    if job_status == "qw":
                        qw_count += 1
    assert True, "To be clear that we are done"

