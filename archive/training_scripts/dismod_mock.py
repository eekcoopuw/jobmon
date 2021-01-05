from jobmon.client.tool import Tool
from jobmon.client.swarm.executors import ExecutorParameters

locs = []
sexes = []
years = []


def foo(task):
    pass


if __name__ == "__main__":

    #
    dismod = Tool("dismod")
    global_template = dismod.get_task_template(
        template_name="global",
        command_template="{python} {mvid} {cv_iter} {add_arguments}",
        node_args=[],
        task_args=["mvid", "cv_iter"],
        op_args=["python", "script", "add_arguments"])

    global_task = global_template.get_task(
        "global")

    child_template = dismod.get_task_template(
        template_name="child",
        command_template="{python} {script} {mvid} {cv_iter} {loc_id} {sex}",
        node_args=["loc_id", "sex", "year"],
        task_args=["mvid", "cv_iter"],
        op_args=["python", "script", "add_arguments"])


    varnish_template = dismod.get_task_template(
        template_name="varnish",
        command_template="{python} {script} {mvid}",
        node_args=[],
        task_args=["mvid"],
        op_args=["python", "script", "add_arguments"])

