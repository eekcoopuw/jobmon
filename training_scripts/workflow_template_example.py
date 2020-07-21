import getpass
import uuid

from jobmon.client.templates.unknown_workflow import UnknownWorkflow as Workflow
from jobmon.client.templates.bash_task import BashTask
from jobmon.client.templates.python_task import PythonTask


def workflow_template_example():
    """
    Instraction:
        This workflow use workflow template (UnkownWorkflow, BashTaks / PythonTasks).
        One of benefit to use template is its compatibility with previous jobmon and new jobmon. (guppy relase)
        It will create node and dag object in jobmon database, it helps to clarify task"s dependant info.
        The flow in this example is:
        1. create workflow (use Unknown Template)
        2. define executor params
        3. create tasks (use BashTask / PythonTask template)
        4. add tasks to workflow
        5. run workflow

    To Run:
        with jobmon installed in your conda environment from the root of the repo, run:
           $ python training_scripts/workflow_template_example.py
    """

    user = getpass.getuser()    
    wf_uuid = uuid.uuid4()

    # create workflow
    workflow = Workflow(
        name = f"template_workflow_{wf_uuid}",
        description = "template_workflow",
        executor_class = "SGEExecutor",
        stderr = f"/ihme/scratch/users/{user}/{wf_uuid}",
        stdout = f"/ihme/scratch/users/{user}/{wf_uuid}",
        project = "proj_scicomp"
    )

    # create tasks
    task1 = BashTask(
        command = "echo task1", 
        executor_class = "SGEExecutor"
    )

    task2 = BashTask(
        command = "echo task2", 
        executor_class = "SGEExecutor",
        upstream_tasks = [task1]
    )

    task3 = PythonTask(
        script = f"/ihme/scratch/users/{user}/script/test.py",
        args = ["--args1", "val1", "--args2", "val2"],
        executor_class = "SGEExecutor",
        upstream_tasks = [task2]
    )

    # add task to workflow
    workflow.add_tasks([task1, task2, task3])

    # run workflow
    workflow.run()


if __name__ == "__main__":
    workflow_template_example()
