from functools import partial
import pytest
from time import sleep

from jobmon.client.swarm.executors import sge_utils


def do_nothing():
    return


def timeout_and_skip(step_size=10, max_time=120, max_qw=1,
                     job_name="sleepyjob",
                     partial_test_function=partial(
        do_nothing)):
    """
    Utility function to wrap a test in a timeout loop. If it exceeds timeouts
    then check if there were qwait states. If so, skip the test because it
    probably timed out due to cluster load.

    Args:
        step_size: number of seconds to wait between subsequent checks
        max_time: timeout in seconds
        max_qw: The max number of times that the test job is allowed to be in
            qw state, otherwise skip the test
        partial_test_function: Actually check the test if it is ready.
        returns true if it ran the test, false otherwise
    """
    total_sleep = 0
    qw_count = 0
    while True:
        print(f"*** Check {job_name}")
        sleep(step_size)
        total_sleep += step_size
        # There should now be a job that has errored out
        if partial_test_function():
            # The test passed, we are good

            print(f"*** Passed {job_name}")
            break
        else:
            # Probe qstat and count the number of qw states.
            # If we aren't making progress then dynamically skip the test.
            # Do this first so that qw_count is correct
            qstat_out = sge_utils.qstat()
            jid = None
            job_status = None
            for id in qstat_out.keys():
                if qstat_out[id]['name'] == job_name:
                    jid = id
                    job_status = qstat_out[id]['status']
                    print(f"found job {jid} with status {job_status}")
            import pdb
            pdb.set_trace()
            if jid:
                # Make sure that job exists
                if job_status == "qw":
                    print("job still in qwait")
                    qw_count += 1

            if total_sleep > max_time:
                if qw_count >= max_qw:
                    # The cluster is having a bad day
                    pytest.skip("Skipping test, saw too many ({}) qw states"
                                .format(max_qw))
                else:
                    qstat_msg = sge_utils.qstat()
                    assert False, \
                        f"timed out "\
                        f"qwait count {qw_count}, " \
                        f"total_sleep {total_sleep}, " \
                        f"max_time {max_time}), might be: " \
                        f"a real bug, cluster load, or " \
                        f"project permissions for 'proj_jenkins'; " \
                        f"qstat {qstat_msg}"
            else:
                print(f"*** Try it again {job_name}")

    # To be clear that we are done and everything passed
    return True
