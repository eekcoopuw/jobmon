from argparse import ArgumentParser, Namespace
from datetime import datetime
import getpass
import os
import random
import shlex
import stat
from typing import Optional
import uuid


from jobmon.client.api import Tool, ExecutorParameters
from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
random.seed(12345)


def three_phase_load_test(n_tasks: int, wfid: str = "", n_taskargs: int = 1,
                          l_com: int = 0, n_edges: int = 0, n_attr: int = 1) -> None:
    """
    Creates and runs one workflow with n_tasks tasks of n_taskargs args
    and length of l_com command and another n_edge jobs that have
    dependencies to the previous n_tasks jobsm then a final tier that has n_tasks jobs
    with multiple dependencies on the 3n job tier. This can test how jobmon
    will respond when there is a large load of connections and communication
    to and from the db (like when dismod or codcorrect run)

                t1-1     t1-2       ...   t1-n_tasks
                 |        |         ...
                t2-1     t2-2       ...   t2-n_edges
                 |\      /|\                /|
                 | \    / | \       ...    / |
                 ...
                 |    /  \|/        ...    \ |
                t3-1     t3-2              t3-n_tasks
    """
    unknown_tool = Tool()

    # construct task template
    command = os.path.join(thisdir, "sleep_and_echo.sh")
    st = os.stat(command)
    os.chmod(command, st.st_mode | stat.S_IXUSR)
    tt = unknown_tool.get_task_template(
        template_name="sleep_and_echo",
        # {thisdir}/sleep_and_echo.sh {sleep_time} {counter} {filler} {wfid1} {wfid2}... {wfidn_taskargs}
        command_template="{thisdir}/sleep_and_echo.sh {sleep_time} {counter} {filler} " + \
                         str(["{wfid" + str(i) + "}" for i in range(n_taskargs)])[1:-1].replace(",", "").replace("\'", ""),
        node_args=["sleep_time", "counter", "filler"],
        task_args=["wfid" + str(k) for k in range(n_taskargs)],
        op_args=["thisdir"]
    )

    if not wfid:
        wfid = str(uuid.uuid4())
    user = getpass.getuser()
    wf = Workflow(f"load-test_{wfid}", "load_test",
                  stderr=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  stdout=f"/ihme/scratch/users/{user}/tests/load_test/{wfid}",
                  project="proj_scicomp")

    tier1 = []
    counter = 0
    # Make command long enough
    filler = "x" * l_com
    # First Tier
    for i in range(n_tasks):
        counter += 1
        sleep_time = random.randint(30, 41)
        # 3  new attributes per task with values to cause full new bind for every attribute
        attributes_t1 = {'foo_t1_'+str(i)+str(k): 'foo_t1_value'+str(k) for k in range(n_attr)}
        tier_1_task = tt.create_task(
            executor_parameters=ExecutorParameters(num_cores=1),
            thisdir=thisdir,
            sleep_time=sleep_time,
            counter=counter,
            filler=filler,
            task_attributes=attributes_t1,
            **{"wfid" + str(k): k for k in range(n_taskargs)}
        )
        tier1.append(tier_1_task)

    tier2 = []
    # Second Tier, depend on 1 tier 1 task
    for i in range(n_edges):
        counter += 1
        sleep_time = random.randint(30, 41)
        # Same 3 attributes for every tier 2 task
        attributes_t2 = {'foo_t2_'+str(i)+str(k): 'foo_t2_value'+str(k) for k in range(n_attr)}
        tier_2_task = tt.create_task(
            executor_parameters=ExecutorParameters(num_cores=1),
            thisdir=thisdir,
            sleep_time=sleep_time,
            counter=counter,
            filler=filler,
            task_attributes=attributes_t2,
            upstream_tasks=[tier1[(i % n_tasks)]],
            **{"wfid" + str(k): k for k in range(n_taskargs)}
        )
        tier2.append(tier_2_task)

    tier3 = []
    # Third Tier, depend on all tier 2 tasks
    for i in range(n_tasks):
        counter += 1
        sleep_time = random.randint(30, 41)
        # each task has n_attr attributes with no value assigned yet
        attributes_t3 = []
        for j in range(n_attr):
            attributes_t3.append(f'foo_t3_{j}')
        tier_3_task = tt.create_task(
            executor_parameters=ExecutorParameters(num_cores=1),
            thisdir=thisdir,
            sleep_time=sleep_time,
            counter=counter,
            filler=filler,
            task_attributes=attributes_t3,
            upstream_tasks=tier2,
            **{"wfid" + str(k): k for k in range(n_taskargs)}
        )
        tier3.append(tier_3_task)

    wf.add_tasks(tier1 + tier2 + tier3)

    num_tasks = len(tier1) + len(tier2) + len(tier3)
    time = datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    print(f"{time}: Beginning the workflow, there are {num_tasks} tasks in "
          f"this DAG")
    wf.run()
    time = datetime.now().strftime("%m/%d/%Y/_%H:%M:%S")
    print(f"{time}: Workflow complete!")


def parse_arguments(argstr: Optional[str] = None) -> Namespace:
    """
    Gets arguments from the command line or a command line string.
    """
    parser = ArgumentParser()
    parser.add_argument("--numTask", type=int, required=True, default=1)
    parser.add_argument("--numTaskArg", type=int, required=True, default=0)
    parser.add_argument("--comLength", type=int, required=True, default=0)
    parser.add_argument("--numEdge", type=int, required=True, default=0)
    parser.add_argument("--numAttr", type=int, required=True, default=1)
    parser.add_argument("--wfid", type=str, required=True)

    if argstr is not None:
        arglist = shlex.split(argstr)
        args = parser.parse_args(arglist)
    else:
        args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_arguments()
    three_phase_load_test(n_tasks=args.numTask, wfid=args.wfid, n_taskargs=args.numTaskArg,
                          l_com=args.comLength, n_edges=args.numEdge, n_attr=args.numAttr)
