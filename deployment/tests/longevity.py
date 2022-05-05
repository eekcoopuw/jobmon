from argparse import ArgumentParser, Namespace
import getpass
import os
import random
import shlex
from subprocess import Popen, TimeoutExpired
import time
from typing import Dict, List, Optional
import yaml


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def generate_random_workflow_yaml(
    output_dir: str,
    wf_name: str = "foo",
    random_seed: int = 123,
    n_tasks: int = 100,
    n_phases: int = 3,
    intermittent_fails: bool = True,
    resource_fails: bool = True,
    fail_always: bool = False
) -> str:

    # generate random values
    random.seed(random_seed)
    phase_n_attr = [random.randint(0, 5) for i in range(0, n_phases)]
    phase_task_ratios = [random.randint(0, 30) for i in range(0, n_phases)]
    phase_percent_in_degree = [0] + [random.randint(0, 100) for i in range(0, n_phases - 1)]
    if intermittent_fails:
        phase_percent_intermittent_fail = [random.randint(0, 30) for i in range(0, n_phases)]
    else:
        phase_percent_intermittent_fail = [0 for i in range(0, n_phases)]
    if resource_fails:
        phase_percent_sleep_timeout = [random.randint(0, 10) for i in range(0, n_phases)]
    else:
        phase_percent_sleep_timeout = [0 for i in range(0, n_phases)]
    if fail_always:
        phase_percent_fail_always = [random.randint(0, 100) for i in range(0, n_phases)]
    else:
        phase_percent_fail_always = [0 for i in range(0, n_phases)]

    config_dict: Dict = {
        "load_test_parameters": {
            wf_name: {
                "random_seed": random_seed,
                "n_tasks": n_tasks,
                "phase_n_attr": phase_n_attr,
                "phase_task_ratios": phase_task_ratios,
                "phase_percent_in_degree": phase_percent_in_degree,
                "phase_percent_intermittent_fail": phase_percent_intermittent_fail,
                "phase_percent_sleep_timeout": phase_percent_sleep_timeout,
                "phase_percent_fail_always": phase_percent_fail_always
            }
        }
    }

    # write to file
    dir_name = os.path.join(output_dir, wf_name)
    os.mkdir(dir_name)
    yaml_path = os.path.join(dir_name, "_wf_def.yaml")
    with open(yaml_path, 'w') as outfile:
        yaml.dump(config_dict, outfile)
    return yaml_path


def popen_workflow(output_dir, wf_name, random_seed) -> Popen:
    yaml_path = generate_random_workflow_yaml(output_dir, wf_name, random_seed)
    cmd = [
        "python", os.path.join(thisdir, "load_test_generator.py"),
        "--yaml_path", yaml_path,
        "--scratch_dir", output_dir,
        "--wfid", wf_name
    ]
    return Popen(cmd)


def parse_arguments(argstr: Optional[str] = None) -> Namespace:
    """Construct a parser, parse either sys.argv (default) or the provided argstr, returns
    a Namespace. The Namespace should have a 'func' attribute which can be used to dispatch
     to the appropriate downstream function.
    """
    parser = ArgumentParser()
    parser.add_argument("--time_in_minutes", type=int, default=60,
                        help="how long in minutes to run the test")
    parser.add_argument("--concurrency", type=int, default=4,
                        help="how many workflows to run in parallel at one time")
    parser.add_argument("--output_dir", required=False,
                        default=f"/ihme/scratch/users/{getpass.getuser()}", type=str,
                        action='store',
                        help="location where yaml, logs and other artifacts will be written")

    arglist: Optional[List[str]] = None
    if argstr is not None:
        arglist = shlex.split(argstr)

    args = parser.parse_args(arglist)
    return args


def main(duration: int, concurrency: int, output_dir: str):
    START_TIME = time.time()
    WFID = 0
    BASE_SEED = random.randint(1, 1000)

    # initialize at concurrency
    wf_procs: List[Popen] = []
    for i in range(concurrency):
        WFID += 1
        wf_procs.append(popen_workflow(output_dir, f"longevity_{WFID}", BASE_SEED + WFID))

    try:
        keep_running = (time.time() - START_TIME) < duration
        while keep_running:
            # sleep to reduce CPU
            time.sleep(0.5)
            keep_running = (time.time() - START_TIME) < duration

            # check next proc status
            proc = wf_procs.pop(0)
            ret_code = proc.poll()
            if ret_code is None:
                wf_procs.append(proc)
            elif ret_code == 0:
                WFID += 1
                wf = popen_workflow(output_dir, f"longevity_{WFID}", BASE_SEED + WFID)
                wf_procs.append(wf)
            elif ret_code != 0:
                keep_running = False

    finally:
        for proc in wf_procs:
            proc.kill()


if __name__ == "__main__":
    args = parse_arguments()

    # Continue creating wf for given times
    main(args.time_in_minutes * 60, args.concurrency, args.output_dir)
