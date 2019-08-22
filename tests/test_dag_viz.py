import os
import subprocess

from cluster_utils.io import makedirs_safely

from jobmon.client.swarm.executors import sge_utils as sge
from jobmon.client.swarm.workflow.task_dag_viz import TaskDagViz
from .mock_sleep_and_write_task import SleepAndWriteFileMockTask

path_to_file = os.path.dirname(__file__)


def test_dag_viz(tmp_out_dir, dag):

    """
    Create a small fork and join dag with four phases:
     a->b[0..2]->c[0..2]->d
     and execute it
    """
    root_out_dir = "{}/mocks/test_fork_and_join_tasks".format(tmp_out_dir)
    makedirs_safely(root_out_dir)
    command_script = sge.true_path(f"{path_to_file}/remote_sleep_and_write.py")

    output_file_name = "{}/a.out".format(root_out_dir)
    task_a = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs 1 --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ofn=output_file_name,
                                     n=output_file_name))
    )
    dag.add_task(task_a)

    # The B's all have varying runtimes,
    task_b = {}
    for i in range(3):
        sleep_secs = 5 + i
        output_file_name = "{}/b-{}.out".format(root_out_dir, i)
        task_b[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_a]
        )
        dag.add_task(task_b[i])

    # Each c[i] depends exactly and only on b[i]
    # The c[i] runtimes invert the b's runtimes, hoping to smoke-out any race
    # conditions by creating a collision near d
    task_c = {}
    for i in range(3):
        sleep_secs = 5 - i
        output_file_name = "{}/c-{}.out".format(root_out_dir, i)
        task_c[i] = SleepAndWriteFileMockTask(
            command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                     "--name {n}".format(cs=command_script, ss=sleep_secs,
                                         ofn=output_file_name,
                                         n=output_file_name)),
            upstream_tasks=[task_b[i]]
        )
        dag.add_task(task_c[i])

    sleep_secs = 3
    output_file_name = "{}/d.out".format(root_out_dir)
    task_d = SleepAndWriteFileMockTask(
        command=("python {cs} --sleep_secs {ss} --output_file_path {ofn} "
                 "--name {n}".format(cs=command_script, ss=sleep_secs,
                                     ofn=output_file_name,
                                     n=output_file_name)),
        upstream_tasks=[task_c[i] for i in range(3)]
    )
    dag.add_task(task_d)

    TaskDagViz(dag, graph_outdir=tmp_out_dir, output_format='svg').render()
    graph_name = '{}/{}.svg'.format(tmp_out_dir, dag.name)
    assert os.path.exists(graph_name)
    node_num = subprocess.check_output(
        """less {} | grep 'class="node"' | wc -l""".format(graph_name),
        shell=True)
    assert int(node_num) == 8
