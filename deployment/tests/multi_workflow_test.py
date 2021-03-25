from argparse import ArgumentParser, Namespace
import os
import shlex
import sys
from typing import List, Optional
from yaml import load, SafeLoader


from jobmon.client.api import Tool, ExecutorParameters
from jobmon.client.swarm.workflow_run import WorkflowRun

thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


def multi_workflow_test(yaml_path: str, scratch_dir: str) -> WorkflowRun:
    """Run a load test with a hypervisor workflow that manages n sub workflows

    Args:
        file_path: the path to a yaml config which specifies each sub workflow
        scratch_dir: location where data artifacts will end up
    """
    yaml_path = os.path.realpath(os.path.expanduser(yaml_path))
    scratch_dir = os.path.realpath(os.path.expanduser(scratch_dir))

    with open(yaml_path) as file:
        params = load(file, Loader=SafeLoader)

    tool = Tool.create_tool("load_tester")
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

    controller_wf = tool.create_workflow(name="controller_wf")
    controller_wf.set_executor(
        stderr=f"{scratch_dir}/controller_wf",
        stdout=f"{scratch_dir}/controller_wf",
        project="proj_scicomp"
    )
    for wfid in params["load_test_parameters"].keys():
        executor_parameters = ExecutorParameters(
            num_cores=3,
            m_mem_free="5G",
            queue='all.q',
        )
        task = task_template.create_task(
            python=sys.executable,
            script=os.path.join(thisdir, "load_test_generator.py"),
            yaml_path=yaml_path,
            scratch_dir=scratch_dir,
            wfid=wfid,
            executor_parameters=executor_parameters
        )
        controller_wf.add_task(task)
    wfr = controller_wf.run()

    return wfr


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


if __name__ == "__main__":

    args = parse_arguments()
    result = multi_workflow_test(args.yaml_path, args.scratch_dir)
    if result.status != 'D':
        raise RuntimeError("Workflow failed")
