from __future__ import annotations

from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
import logging
import os
import random
import shlex
import sys
from typing import Dict, List, Optional, Sequence
from yaml import load, SafeLoader

from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@dataclass
class LoadTestParameters:
    """Container class for storing and validating load test parameters.

    Args:
        wfid: unique identifier for this load test
        cluster_name: cluster name under which this test is conducted
        random_seed: random seed for probabilistic params. set for repeatability
        n_tasks: approximate number of tasks to include in workflow
        phase_task_ratios:  approximate ratio of task size between phases
        phase_n_attr: number of attributes per phase
        phase_percent_in_degree: percentage of previous phase tasks that will be upstream nodes
            into next phase. first element in list must always be 0
        phase_percent_intermittent_fail: the probability that a task in a phase will fail once
            before succeeding
        phase_percent_sleep_timeout: the probability that a task in a phase will fail due to
            over runtime before succeeding
        phase_percent_fail_always: the probability that a task in a phase will always fail
            until no retries are left
    """

    wfid: str
    cluster_name: str
    random_seed: int
    n_tasks: int
    phase_task_ratios: Sequence[int] = field(default=(10, 30, 1))
    phase_n_attr: Sequence[int] = field(default=(10, 5, 0))
    phase_percent_in_degree: Sequence[int] = field(default=(0, 1, 100))
    phase_percent_intermittent_fail: Sequence[int] = field(default=(15, 10, 30))
    phase_percent_sleep_timeout: Sequence[int] = field(default=(10, 5, 0))
    phase_percent_fail_always: Sequence[int] = field(default=(0, 0, 0))

    def __post_init__(self):
        length = len(self.phase_task_ratios)

        # check that phase in degree is 0 for first phase
        if len(self.phase_percent_in_degree) == length:
            if self.phase_percent_in_degree[0] != 0:
                raise ValueError("phase_percent_in_degree must be 0 for first phase")

        match_length_lists = [self.phase_percent_intermittent_fail,
                              self.phase_percent_sleep_timeout, self.phase_percent_fail_always,
                              self.phase_percent_in_degree]
        if any(len(lst) != length for lst in match_length_lists):
            raise ValueError("all phase prefixed arguments must be the same length")

        if any(max(lst) > 100 for lst in match_length_lists):
            raise ValueError("all percent arguments must be less than 100")

    @classmethod
    def from_yaml_file(cls, file_path: str, wfid: str, cluster_name: str = 'slurm') -> LoadTestParameters:
        """Instantiate from a yaml file

        Args:
            file_path: the path to a yaml config
            wfid: the key to use in that yaml for this object
        """
        with open(file_path) as file:
            params = load(file, Loader=SafeLoader)
        return cls(wfid, cluster_name, **params["load_test_parameters"][wfid])


class LoadTestGenerator:

    thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
    script = os.path.join(thisdir, "sleep_and_error.py")

    def __init__(self, scratch_dir: str, wfid: str, cluster_name: str = 'slurm'):
        """Factory for generating a single load test parameterized by LoadTestParameters

        Args:
            scratch_dir: location where data artifacts will end up
            wfid: id used to identify this load test
        """
        # tool attribute
        self.tool = Tool("load_tester")
        self.scratch_dir = os.path.join(scratch_dir, wfid)
        try:
            os.mkdir(self.scratch_dir)
        except FileExistsError:
            pass
        self.workflow = self.tool.create_workflow(name='generated_workflow',
            default_cluster_name=cluster_name,
            default_compute_resources_set={
                cluster_name: {
                    'stderr': f"{scratch_dir}/err",
                    'stdout': f"{scratch_dir}/out",
                    'project': "proj_scicomp"}
                })

        # task_template attributes
        self.task_templates_by_phase: Dict[int, TaskTemplate] = {}
        self.tasks_by_phase: Dict[int, List[Task]] = {}

        # task generation attributes
        self.counter = 0

    @classmethod
    def from_parameters(cls, scratch_dir: str, parameters: LoadTestParameters
                        ) -> LoadTestGenerator:
        """Generate a load test from an instance of LoadTestParameters

        Args:
            scratch_dir: location where data artifacts will end up
            parameters: specification object for this load test
        """
        random.seed(parameters.random_seed)
        ltg = cls(scratch_dir=scratch_dir, wfid=parameters.wfid,
                  cluster_name=parameters.cluster_name)
        ltg.add_tasks_to_workflow(
            total_tasks=parameters.n_tasks,
            phase_task_ratios=list(parameters.phase_task_ratios),
            phase_n_attr=list(parameters.phase_n_attr),
            phase_percent_in_degree=list(parameters.phase_percent_in_degree),
            phase_percent_intermittent_fail=list(parameters.phase_percent_intermittent_fail),
            phase_percent_sleep_timeout=list(parameters.phase_percent_sleep_timeout),
            phase_percent_fail_always=list(parameters.phase_percent_fail_always)
        )
        random.seed()
        return ltg

    def create_task_template(self, phase: int):
        template_name = f"phase_{phase}_template"

        command_template = (
            "{python} {script} "
            "--uid {uid} "
            "--sleep_secs {sleep_secs} "
            "--fail_count {fail_count} "
            "--fail_count_fp {fail_count_fp} "
            "{fail_always} {sleep_timeout}"
        )

        self.task_templates_by_phase[phase] = self.tool.get_task_template(
            template_name=template_name,
            command_template=command_template,
            node_args=["uid"],
            task_args=["sleep_secs", "fail_count", "fail_count_fp", "fail_always",
                       "sleep_timeout"],
            op_args=["python", "script"]
        )
        self.tasks_by_phase[phase] = []

    def add_tasks_to_workflow(self, total_tasks: int, phase_task_ratios: List[int],
                              phase_n_attr: List[int], phase_percent_in_degree: List[int],
                              phase_percent_intermittent_fail: List[int],
                              phase_percent_sleep_timeout: List[int],
                              phase_percent_fail_always: List[int]):
        # calculate proportion of total tasks per phase
        phase_multiplier = float(total_tasks) / float(sum(phase_task_ratios))

        # loop through phases
        for phase in range(len(phase_task_ratios)):
            # create task template
            self.create_task_template(phase)

            # create tasks
            tasks = self._get_tasks_by_phase(
                phase=phase,
                n_tasks=int(phase_task_ratios[phase] * phase_multiplier),
                in_degree_percent=int(phase_percent_in_degree[phase]),
                n_attr=phase_n_attr[phase],
                percent_intermittent_fail=phase_percent_intermittent_fail[phase],
                percent_sleep_timeout=phase_percent_sleep_timeout[phase],
                percent_fail_always=phase_percent_fail_always[phase]
            )
            self.workflow.add_tasks(tasks)

    def _get_tasks_by_phase(self, phase: int, n_tasks: int, in_degree_percent: int = 10,
                            n_attr: int = 4, percent_intermittent_fail: int = 50,
                            percent_sleep_timeout: int = 50, percent_fail_always: int = 50
                            ) -> List[Task]:
        # First Tier
        task_template = self.task_templates_by_phase[phase]

        for i in range(n_tasks):
            logger.info(f"Create task for phase {phase}. Task number={i}")

            # task args
            self.counter += 1
            sleep_time = random.randint(30, 41)
            compute_resources = {
                'cores': 1,
                'memory': 1,
                'queue': 'all.q',
                'runtime': sleep_time + 20
            }

            # get upstreams unless in degree is 0 (initial condition)
            if in_degree_percent != 0:
                in_degree = len(self.tasks_by_phase[phase - 1]) * (in_degree_percent / 100)
                upstream_tasks = random.sample(self.tasks_by_phase[phase - 1], int(in_degree))
            else:
                upstream_tasks = []

            # set up attribute
            attributes = {f'foo_t1_{i}_{k}': f'foo_t1_value_{k}' for k in range(n_attr)}

            # whether to intermittently fail
            if random.random() < (percent_intermittent_fail / 100):
                n_fails = 1
            else:
                n_fails = 0
            fail_count_fp = os.path.join(self.scratch_dir, str(self.counter))

            # whether to sleep timeout (resource error)
            if random.random() < (percent_sleep_timeout / 100):
                sleep_timeout = "--sleep_timeout"
            else:
                sleep_timeout = ""

            # whether to always fail
            if random.random() < (percent_fail_always / 100):
                fail_always = "--fail_always"
            else:
                fail_always = ""

            task = task_template.create_task(
                python=sys.executable,
                script=self.script,
                uid=self.counter,
                sleep_secs=sleep_time,
                fail_count=n_fails,
                fail_count_fp=fail_count_fp,
                sleep_timeout=sleep_timeout,
                fail_always=fail_always,
                task_attributes=attributes,
                compute_resources=compute_resources,
                upstream_tasks=upstream_tasks,
                max_attempts=2
            )
            logging.info(f"Task {i} created")
            self.tasks_by_phase[phase].append(task)

        return self.tasks_by_phase[phase]


def parse_arguments(argstr: Optional[str] = None) -> Namespace:
    """Construct a parser, parse either sys.argv (default) or the provided argstr, returns
    a Namespace. The Namespace should have a 'func' attribute which can be used to dispatch
     to the appropriate downstream function.
    """
    parser = ArgumentParser()
    parser.add_argument("--yaml_path", required=True, type=str, action='store',
                        help=("path to a yaml file such as the one found in "
                              "jobmon/deployment/tests/sample.yaml"))
    parser.add_argument("--wfid", required=True, type=str, action='store',
                        help="a key in the yaml_path for a specific workflow entry")
    # There is a consistency issue going on between Jobmon core and Load_test,
    # actually more on the Jobmon core side: If we try to pass cluster_name via Jobmon core,
    # Jobmon core has some inconsistency with kwargs and named --cluster_name parameter.
    # I don't want to change Jobmon core at the moment, so I commented this out to get it going.
    # Related TBD: GBDSCI-4277: cluster_name pass-through does not work as expected
    # parser.add_argument("--cluster_name", required=True, type=str, action='store',
    #                     help="cluster name that this test is conducted on")
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
    params = LoadTestParameters.from_yaml_file(args.yaml_path, args.wfid)
    load_test_generator = LoadTestGenerator.from_parameters(args.scratch_dir, params)
    load_test_generator.workflow.run()
