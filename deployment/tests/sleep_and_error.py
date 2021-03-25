import argparse
import os
import time

from getpass import getuser


def main():
    """
    The remote script that MockSleepAndWrite calls
    """

    parser = argparse.ArgumentParser(description='mock job')
    parser.add_argument('--sleep_secs',
                        type=int, default=10,
                        action='store',
                        help='The number of seconds to sleep before writing '
                             'or dying')
    parser.add_argument('--fail_always',
                        action='store_true', default=False,
                        help='If true, sleep and then raise a ValueError')
    parser.add_argument('--fail_count',
                        type=int, default=0,
                        action='store',
                        help='If true, then check the a file for the count of '
                             'previous failures (stateful!).'
                             'If previous fails < fail_count then fail')
    parser.add_argument('--sleep_timeout',
                        action='store_true', default=False,
                        help='If fail count is set so that jobs fail a few '
                             'times you can either configure them to throw a '
                             'value error or sleep longer than the given '
                             'timeout')
    parser.add_argument('--uid',
                        type=str, default='uuid',
                        action='store',
                        help='If set, commands will be made unique so that '
                             'you can test the same command (sleep) in many '
                             'iterations without getting warnings')
    parser.add_argument('--fail_count_fp',
                        type=str,
                        default=f'/tmp/jobmon-test',
                        action='store',
                        help='filepath to count how many failures have '
                               'occurred')
    args = parser.parse_args()

    if args.fail_always:
        time.sleep(args.sleep_secs)
        raise ValueError("Mock task failing permanently by command line arg")
    elif args.fail_count:
        # Go check how many times this script has failed
        counter_file = "{}-count_{}".format(args.fail_count_fp, args.uid)
        if os.path.exists(counter_file):
            # Not the first time, let's see how many times we have failed
            fp = open(counter_file, "r")
            count_so_far = int(fp.read())
            fp.close()
        else:
            # First time, have not yet failed
            count_so_far = 0
        if count_so_far < args.fail_count:
            # Have not yet failed enough
            count_so_far += 1
            fp = open(counter_file, "w")
            fp.write("{}\n".format(count_so_far))
            fp.close()
            if args.sleep_timeout:
                time.sleep(300)  # set to sleep longer than the max runtime
            else:
                raise ValueError(f"Programmed intermittent failures, total "
                                 f"failures so far: {count_so_far}")
        else:
            # Enough failures, we should succeed this time
            time.sleep(args.sleep_secs)
    else:
        # No "fail" argument, we should succeed
        time.sleep(args.sleep_secs)


if __name__ == "__main__":
    main()
