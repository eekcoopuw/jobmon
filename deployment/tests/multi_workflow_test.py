from argparse import ArgumentParser, Namespace
import os
import shlex
import sys
from typing import List, Optional
from yaml import load, SafeLoader
import uuid

from jobmon.client.api import Tool

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def multi_workflow_test(yaml_path: str, scratch_dir: str) -> str:
    """Run a load test with a hypervisor workflow that manages n sub workflows

    Args:
        file_path: the path to a yaml config which specifies each sub workflow
        scratch_dir: location where data artifacts will end up
    """
    yaml_path = os.path.realpath(os.path.expanduser(yaml_path))
    scratch_dir = os.path.realpath(os.path.expanduser(scratch_dir))

    with open(yaml_path) as file:
        params = load(file, Loader=SafeLoader)

    tool = Tool("load_tester")
    command_template = (
        "{python} {script} --yaml_path {yaml_path} --scratch_dir {scratch_dir} --wfid {wfid}"
    )
    task_template = tool.get_task_template(
        template_name="workflow_task",
        command_template=command_template,
        node_args=["wfid"],
        task_args=["yaml_path", "scratch_dir"],
        op_args=["python", "script"]
    )

    controller_wf = tool.create_workflow(
        name="controller_wf",
        default_cluster_name='slurm',
        default_compute_resources_set={
            'slurm': {
                'stderr': f"{scratch_dir}/controller_wf",
                'stdout': f"{scratch_dir}/controller_wf",
                'project': "proj_scicomp",
                'cores': 3,
                'memory': "3G",
                'queue': 'all.q'
            }
        }
    )

    for wfid in params["load_test_parameters"].keys():

        task = task_template.create_task(
            python=sys.executable,
            script=os.path.join(thisdir, "load_test_generator.py"),
            yaml_path=yaml_path,
            scratch_dir=scratch_dir,
            wfid=wfid
        )
        controller_wf.add_task(task)
    wfr_status = controller_wf.run()

    return wfr_status


def parse_arguments(argstr: Optional[str] = None) -> Namespace:
    """Construct a parser, parse either sys.argv (default) or the provided argstr, returns
    a Namespace. The Namespace should have a 'func' attribute which can be used to dispatch
     to the appropriate downstream function.
    """
    parser = ArgumentParser()
    parser.add_argument("--yaml_path", required=True, type=str, action='store',
                        help=("path to a yaml file such as the one found in "
                              "jobmon/deployment/tests/sample.yaml"))
    parser.add_argument("--scratch_dir", required=False, default="", type=str, action='store',
                        help="location where logs and other artifacts will be written")

    arglist: Optional[List[str]] = None
    if argstr is not None:
        arglist = shlex.split(argstr)

    args = parser.parse_args(arglist)

    if not args.scratch_dir:
        args.scratch_dir = os.path.dirname(os.path.realpath(args.yaml_path))

    return args


######################
#   Loadtest Main Entry Point
#   1. Create a conda environ with jobmon core installed, and activate it while on a cluster node;
#   2. Navigate to this file's directory;
#   3. Set sample.yaml to reflect your desired testing preferences;
#   4. Run the following command, adjusting the 2 arguments for yourself
#       python multi_workflow_test.py --yaml_path ./sample.yaml --scratch_dir ./scratch
######################
if __name__ == "__main__":

    args = parse_arguments()
    result = multi_workflow_test(args.yaml_path, args.scratch_dir)
    if result != 'D':
        raise RuntimeError("Workflow failed")
