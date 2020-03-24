import pytest

from jobmon.client.swarm.executors.base import ExecutorParameters
from jobmon.client.templates.bash_task import BashTask
from jobmon.client.templates.unknown_workflow import Workflow


def test_task_attribute(db_cfg):

    # create a task
    task = BashTask("sleep 1", num_cores=1)

    # add an attribute to the task
    task.add_task_attribute("num_draws", 10)

    task.bind()

    # query from job_attribute table
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task_attribute_query = DB.session.execute("""
                                SELECT task_attribute.id,
                                       task_attribute.task_id,
                                       task_attribute_type.name,
                                       task_attribute.value
                                FROM task_attribute
                                JOIN task_attribute_type 
                                ON task_attribute.attribute_type = task_attribute_type.id
                                WHERE task_attribute.task_id={id}
                                """.format(id=task.task_id))
        DB.session.commit()
        attribute_entry = task_attribute_query.fetchone()
        attribute_entry_type = attribute_entry.name
        attribute_entry_value = attribute_entry.value

        assert attribute_entry_type == "num_draws"
        assert attribute_entry_value == "10"


def test_static_task_attribute(db_cfg):
    task1 = BashTask("sleep 2", num_cores=1,
                     task_attributes={'num_locations': 32, 'num_years': 3})
    task2 = BashTask("sleep 3", num_cores=1,
                     task_attributes=["num_cores", "num_years"])
    task1.bind()
    task2.bind()

    attributes1 = task1.requester.send_request(
        app_route=f'/task/{task1.task_id}/get_task_attributes',
        message = {},
        request_type='get'
    )
    attributes2 = task2.requester.send_request(
        app_route=f'/task/{task2.task_id}/get_task_attributes',
        message={},
        request_type='get'
    )

    # app = db_cfg["app"]
    # DB = db_cfg["DB"]
    # with app.app_context():
    #     attributes = DB.session.execute("""
    #                     SELECT task_attribute.id,
    #                            task_attribute.task_id,
    #                            task_attribute_type.name,
    #                            task_attribute.value
    #                     FROM task_attribute
    #                     JOIN task_attribute_type
    #                     ON task_attribute.attribute_type = task_attribute_type.id
    #                     WHERE task_attribute.task_id IN ({id1}, {id2})
    #                     """).format(id1=task1.task_id, id2=task2.task_id)
    #     DB.session.commit()


def test_task_attribute_dynamic():
    wf = Workflow("test_dynamic_attrs")
    task1 = BashTask("sleep 1", num_cores=1, executor_parameters=exec_params,
                     task_attributes=["num_years", "num_locations"])
    wf.add_task(task1)
    wf.execute()


def exec_params(task):
    """function to imitate callable for executor parameters that could also
    assign the attributes for the task"""
    num_years = 3
    num_locations = 4
    task.add_attribute("num_years", num_years)
    task.add_attribute("num_locations", num_locations)
    return ExecutorParameters(num_cores=1, m_mem_free='1G',
                              max_runtime_seconds=60)