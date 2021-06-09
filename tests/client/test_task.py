import os

from jobmon.client.distributor.strategies.base import ExecutorParameters
from jobmon.client.task import Task
from jobmon.client.tool import Tool

import pytest

from sqlalchemy.sql import text


@pytest.fixture
def task_template(db_cfg, client_env):
    tool = Tool.create_tool(name="unknown")
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    return tt


def test_good_names():
    """tests that a few legal names return as valid"""
    assert Task.is_valid_job_name("fred")
    assert Task.is_valid_job_name("fred123")
    assert Task.is_valid_job_name("fred_and-friends")


def test_bad_names():
    """tests that invalid names return a ValueError"""
    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("")
    assert "None" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("16")
    assert "digit" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        Task.is_valid_job_name("bad/dog")
    assert "special" in str(exc.value)


def test_equality(task_template):
    """tests that 2 identical tasks are equal and that non-identical tasks
    are not equal"""
    params = ExecutorParameters(executor_class="DummyExecutor")
    a = task_template.create_task(arg="a", executor_parameters=params)
    a_again = task_template.create_task(arg="a", executor_parameters=params)
    assert a == a_again

    b = task_template.create_task(arg="b", upstream_tasks=[a, a_again],
                                  executor_parameters=params)
    assert b != a
    assert len(b.node.upstream_nodes) == 1


def test_hash_name_compatibility(task_template):
    """test that name based on hash"""
    params = ExecutorParameters(executor_class="DummyExecutor")
    a = task_template.create_task(arg="a", executor_parameters=params)
    assert "task_" + str(hash(a)) == a.name


def test_bash_task_equality(client_env):
    """test that two bash tasks with the same command are equal"""

    from jobmon.client.templates.bash_task import BashTask

    a = BashTask(command="echo Hello World")
    a_again = BashTask(command="echo Hello World")

    b = BashTask(command="echo Hello Jobmon", upstream_tasks=[a, a_again])

    assert a == a_again
    assert b != a
    assert len(b.node.upstream_nodes) == 1


def test_hashing_bash_characters(client_env):
    """test that bash characters can be hashed"""
    from jobmon.client.templates.bash_task import BashTask

    a = BashTask(command="touch ~/mytestfile")
    assert a.is_valid_job_name(a.name)


def test_bash_task_bind(db_cfg, client_env):
    """test that all task information gets propagated appropriately into the db
    """
    from jobmon.client.templates.bash_task import BashTask
    from jobmon.server.web.models.task import Task
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    workflow1 = UnknownWorkflow(name="test_bash_task_bind",
                                executor_class="SequentialExecutor")

    task1 = BashTask(command="echo 'Hello Jobmon'", max_attempts=1,
                     executor_class="DummyExecutor")
    workflow1.add_tasks([task1])
    workflow1.bind()
    workflow1._create_workflow_run()
    bound_task = workflow1.tasks[hash(task1)]

    with app.app_context():
        task = DB.session.query(Task).filter_by(id=task1.task_id).one()

        # check all task args
        assert task.workflow_id == workflow1.workflow_id
        assert task.node_id == bound_task.node.node_id
        assert task.name == bound_task.name
        assert task.command == bound_task.command
        assert task.num_attempts == 0
        assert task.max_attempts == bound_task.max_attempts

        DB.session.commit()


def test_bash_task_command_parsing(db_cfg, client_env):
    from jobmon.client.api import BashTask, UnknownWorkflow
    bash_wf = UnknownWorkflow(name="test_bash_task_parsing", executor_class="DummyExecutor")
    bash_a = BashTask('OP_NUM_THREADS 1 echo hi && sleep 3', task_args={'echo_str': 'hi'},
                      node_args={'sleep': 3})
    bash_b = BashTask('OP_NUM_THREADS 1 echo boo && sleep 5', task_args={'echo_str': 'boo'},
                      node_args={'sleep': 5})
    bash_c = BashTask('echo blah && sleep 6', task_args={'echo_str': 'blah'},
                      node_args={'sleep': 6}, env_variables={'OP_NUM_THREADS': '1'})
    bash_wf.add_tasks([bash_a, bash_b, bash_c])
    bash_wf.bind()
    bash_wf._create_workflow_run()

    bound_a = bash_wf.tasks[hash(bash_a)]
    bound_b = bash_wf.tasks[hash(bash_b)]
    bound_c = bash_wf.tasks[hash(bash_c)]
    assert bound_a.task_id != bound_b.task_id
    assert bound_a.node.task_template_version_id == bound_b.node.task_template_version_id
    assert list(bound_a.task_args.values())[0] == 'hi'
    assert list(bound_b.task_args.values())[0] == 'boo'
    assert bound_c.command == 'OP_NUM_THREADS=1 echo blah && sleep 6'


def test_python_task_command_parsing(db_cfg, client_env):
    from jobmon.client.api import PythonTask, UnknownWorkflow
    wf = UnknownWorkflow(name="test_python_task_parsing", executor_class="DummyExecutor")
    py_a = PythonTask(name="task_a", script='~/runme.py',
                      args=['--blah', 3, '--bop', 2, '--hop', 5, '--baz', '4'])
    py_b = PythonTask(name="task_a", script='~/runme.py',
                      args=['--blah', 4, '--bop', 6, '--hop', 5, '--baz', '4'])
    wf.add_tasks([py_a, py_b])
    wf.bind()
    wf._create_workflow_run()
    bound_a = wf.tasks[hash(py_a)]
    bound_b = wf.tasks[hash(py_b)]
    assert bound_a.task_id != bound_b.task_id
    assert bound_a.node.task_template_version_id == bound_b.node.task_template_version_id


def test_python_task_equality(db_cfg, client_env):
    """Test that two identical python tasks are equal and that a non-identical
    task is not equal"""
    from jobmon.client.templates.python_task import PythonTask

    a = PythonTask(script='~/runme.py', args=[1])
    a_again = PythonTask(script='~/runme.py', args=[1])
    assert a == a_again

    b = PythonTask(script='~/runme.py', args=[2], upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.node.upstream_nodes) == 1
    assert b.node.task_template_version_id == a.node.task_template_version_id


def test_python_task_args(db_cfg, client_env):
    """test that env_variables and other arguments are handled appropriately
    by python task"""
    from jobmon.client.templates.python_task import PythonTask
    from jobmon.server.web.models.task import Task
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    import sys

    app = db_cfg["app"]
    DB = db_cfg["DB"]

    workflow1 = UnknownWorkflow(name="test_python_task_args",
                                executor_class="SequentialExecutor")

    task1 = PythonTask(script='~/runme.py', env_variables={'OP_NUM_THREADS': 1},
                       num_cores=1, m_mem_free='2G', max_attempts=1)
    workflow1.add_tasks([task1])
    workflow1.bind()
    workflow1._create_workflow_run()
    bound_task = workflow1.tasks[hash(task1)]

    with app.app_context():
        task = DB.session.query(Task).filter_by(id=task1.task_id).one()

        # check all task args
        assert task.workflow_id == workflow1.workflow_id
        assert task.node_id == bound_task.node.node_id
        assert task.name == bound_task.name
        assert task.command == bound_task.command
        assert task.num_attempts == 0
        assert task.max_attempts == bound_task.max_attempts

        # check all job args
        assert bound_task.command == f'OP_NUM_THREADS=1 {sys.executable} ~/runme.py'


def test_task_attribute(db_cfg, client_env):
    """Test that you can add task attributes to Bash and Python tasks"""
    from jobmon.client.api import BashTask
    from jobmon.client.api import PythonTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.server.web.models.task_attribute import TaskAttribute
    from jobmon.server.web.models.task_attribute_type import TaskAttributeType

    workflow1 = UnknownWorkflow(name="test_task_attribute",
                                executor_class="SequentialExecutor")
    executor_parameters = ExecutorParameters(m_mem_free='1G', num_cores=1, queue='all.q',
                                             executor_class="SequentialExecutor")
    task1 = BashTask("sleep 2",
                     task_attributes={'LOCATION_ID': 1, 'AGE_GROUP_ID': 5, 'SEX': 1},
                     executor_parameters=executor_parameters)

    this_file = os.path.dirname(__file__)
    script_path = os.path.abspath(os.path.expanduser(
        f"{this_file}/../_scripts/remote_sleep_and_write.py"))

    task2 = PythonTask(script=script_path, num_cores=1,
                       task_attributes=["NUM_CORES", "NUM_YEARS"])

    task3 = BashTask("sleep 3", num_cores=1, task_attributes={'NUM_CORES': 3, 'NUM_YEARS': 5},
                     executor_parameters=executor_parameters)
    workflow1.add_tasks([task1, task2, task3])
    workflow1.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT task_attribute_type.name, task_attribute.value, task_attribute_type.id
        FROM task_attribute
        INNER JOIN task_attribute_type
            ON task_attribute.task_attribute_type_id = task_attribute_type.id
        WHERE task_attribute.task_id IN (:task_id_1, :task_id_2, :task_id_3)
        ORDER BY task_attribute_type.name, task_id
        """
        resp = DB.session.query(TaskAttribute.value, TaskAttributeType.name,
                                TaskAttributeType.id).\
            from_statement(text(query)).params(task_id_1=task1.task_id,
                                               task_id_2=task2.task_id,
                                               task_id_3=task3.task_id).all()
        values = [tup[0] for tup in resp]
        names = [tup[1] for tup in resp]
        ids = [tup[2] for tup in resp]
        expected_vals = ['5', '1', None, '3', None, '5', '1']
        expected_names = ['AGE_GROUP_ID', 'LOCATION_ID', 'NUM_CORES', 'NUM_CORES', 'NUM_YEARS',
                          'NUM_YEARS', 'SEX']

        assert values == expected_vals
        assert names == expected_names
        assert ids[2] == ids[3]  # will fail if adding non-unique task_attribute_types
        assert ids[4] == ids[5]
