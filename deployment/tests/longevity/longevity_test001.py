from time import time
from random import randint
import os
from threading import Thread, Lock
import logging
import sys

from jobmon.client.api import Tool, ExecutorParameters
from jobmon.client.execution.strategies.sge.sge_executor import SGEExecutor

"""
This script creates workflows with random tasks and random task dependencies,
and starts multiple threads to continually add new workflows in a given time.
"""
NAME_PREFIX = "LXMAXPSS"
MAX_TIERS = 4
MAX_TASKS_IN_TIERS = 500
MIN_TASKS_IN_TIERS = 1
TOTAL_THREADS = 8
INCLUDE_FAILED = False

MYDIR = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))
MYSCRIPT = "{}/sleeprandom.sh".format(MYDIR)

START_TIME = time()
LOCK = Lock()

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)


# This class generates a unique number to be used as wf args
class NameGenerator:
    _counter = 0

    @staticmethod
    def get():
        LOCK.acquire()
        NameGenerator._counter += 1
        # mimic real sge task run
        LOCK.release()
        return NameGenerator._counter


def random_pick_from_lists(lists, ran_max=2):
    return_list = []
    for list in lists:
        for a in list[0:-1]:
            if randint(0, ran_max) == 1:
                return_list.append(a)
    return return_list


def create_task_template():
    t = Tool()
    tt = t.get_task_template(
        template_name="random_sleep",
        command_template=(
            "sh "
            "{script} "
            "{tier} "
            "{num} "),
        node_args=["tier", "num"],
        task_args=[],
        op_args=["script"])

    # both the tool and the task template are returned
    return(t, tt)


def create_wf():
    # max tier 5
    wf_name = "longevity-test-{}".format(NameGenerator.get())
    wf_name = NAME_PREFIX + wf_name

    tool, template = create_task_template()
    wf = tool.create_workflow(name=wf_name)
    wf.set_executor(executor_class="SGEExecutor", project="proj_scicomp")
    logger.debug("Workflow {} created".format(wf_name))

    tiers = randint(1, 6)
    logger.debug("Tiers: {}".format(tiers))
    tasks = []
    for i in range(0, tiers):
        logger.debug("Tier {}".format(i))
        tasks.append([])
        for t in range(0, randint(MIN_TASKS_IN_TIERS, MAX_TASKS_IN_TIERS)):
            upstream_tasks = None
            if i > 0:
                upstream_tasks = random_pick_from_lists(tasks[:i])
                # always include one member of the previous tier
                upstream_tasks.append(tasks[i - 1][-1])
            bash_task = template.create_task(
                executor_parameters=ExecutorParameters(
                    queue="all.q",
                    num_cores=1,
                    m_mem_free="1G",
                    executor_class="SGEExecutor"),
                name="task_tier_{}_val_{}".format(i, t),
                tier=i,
                num=t,
                max_attempts=1,
                script=MYSCRIPT)
            tasks[i].append(bash_task)
    # add all tasks to one array
    all_tasks = []
    for t in tasks:
        all_tasks = all_tasks + t
    wf.add_tasks(all_tasks)
    wf.run()


def create_simple_wf():
    wf_name = "simple-wf-{}".format(NameGenerator.get())
    wf_name = NAME_PREFIX + wf_name

    tool, template = create_task_template()
    wf = tool.create_workflow(name=wf_name)
    wf.set_executor(executor_class="SGEExecutor", project="proj_scicomp")

    tasks = []
    tier = 1
    for i in range(5):
        task = template.create_task(
            executor_parameters=ExecutorParameters(
                queue="all.q",
                num_cores=1,
                m_mem_free="1G",
                executor_class="SGEExecutor"),
            name="task_tier_{}_val_{}".format(tier, i),
            tier=tier,
            num=i,
            max_attempts=1,
            script=MYSCRIPT)
        tasks.append(task)
    wf.add_tasks(tasks)
    wf.run()


def loop(duration):
    while time() - START_TIME < duration:
        create_wf()


if __name__ == "__main__":
    time_in_minutes = 10
    if len(sys.argv) > 1:
        time_in_minutes = int(sys.argv[1])

    if INCLUDE_FAILED:
        os.system("chmod 400 {}".format(MYSCRIPT))
        create_simple_wf()
    os.system("chmod 755 {}".format(MYSCRIPT))

    # Continue creating wf for given times
    time_in_seconds = time_in_minutes * 60
    threads = []
    for i in range(TOTAL_THREADS):
        t = Thread(target=loop, args=(time_in_seconds))
        threads.append(t)
        t.start()
    for t in threads:
        t.join(time_in_seconds + 360)
