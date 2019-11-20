import pytest

from jobmon.client.workflow.tool import Tool
from jobmon.client.workflow.task import Task
from jobmon.client.swarm.executors.base import ExecutorParameters


@pytest.fixture
def task_template(db_cfg, env_var):
    tool = Tool.create_tool(name="unknown")
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[])
    return tt


def test_good_names():
    assert Task.is_valid_job_name("fred")
    assert Task.is_valid_job_name("fred123")
    assert Task.is_valid_job_name("fred_and-friends")


def test_bad_names():
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
    params = ExecutorParameters(executor_class="DummyExecutor")
    a = task_template.create_task(arg="a", executor_parameters=params)
    a_again = task_template.create_task(arg="a", executor_parameters=params)
    assert a == a_again

    b = task_template.create_task(arg="b", upstream_tasks=[a, a_again],
                                  executor_parameters=params)
    assert b != a
    assert len(b.node.upstream_nodes) == 1


def test_hash_name_compatibility(task_template):
    params = ExecutorParameters(executor_class="DummyExecutor")
    a = task_template.create_task(arg="a", executor_parameters=params)
    assert "task_" + str(hash(a)) == a.name


def test_bash_task_equality(env_var):
    from jobmon.client.workflow.templates.bash_task import BashTask

    a = BashTask(command="echo 'Hello World'")
    a_again = BashTask(command="echo 'Hello World'")
    assert a == a_again

    b = BashTask(command="echo 'Hello Jobmon'", upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.node.upstream_nodes) == 1


def test_hashing_bash_characters(env_var):
    from jobmon.client.workflow.templates.bash_task import BashTask

    a = BashTask(command="touch ~/mytestfile")
    assert a.is_valid_job_name(a.name)


def test_bash_task_bind(db_cfg, env_var):
    from jobmon.client.workflow.templates.bash_task import BashTask
    from jobmon.models.task import Task
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    a = BashTask(command="echo 'Hello Jobmon'", max_attempts=1,
                 executor_class="DummyExecutor")
    a.workflow_id = 1
    a.node.bind()
    a.bind()

    with app.app_context():
        task = DB.session.query(Task).filter_by(id=a.task_id).one()

        # check all task args
        assert task.workflow_id == 1
        assert task.node_id == a.node.node_id
        assert task.name == a.name
        assert task.command == a.command
        assert task.num_attempts == 0
        assert task.max_attempts == a.max_attempts

        DB.session.commit()


def test_python_task_equality(env_var):
    from jobmon.client.workflow.templates.python_task import PythonTask

    a = PythonTask(script='~/runme.py', args=[1])
    a_again = PythonTask(script='~/runme.py', args=[1])
    assert a == a_again

    b = PythonTask(script='~/runme.py', args=[2], upstream_tasks=[a, a_again])
    assert b != a
    assert len(b.node.upstream_nodes) == 1
    assert b.node.task_template_version_id == a.node.task_template_version_id


def test_python_task_args(db_cfg, env_var):
    from jobmon.client.workflow.templates.python_task import PythonTask
    from jobmon.models.task import Task
    import sys

    app = db_cfg["app"]
    DB = db_cfg["DB"]

    a = PythonTask(script='~/runme.py', env_variables={'OP_NUM_THREADS': 1},
                   num_cores=1, m_mem_free='2G', max_attempts=1)
    a.workflow_id = 1
    a.node.bind()
    a.bind()

    with app.app_context():
        task = DB.session.query(Task).filter_by(id=a.task_id).one()

        # check all task args
        assert task.workflow_id == 1
        assert task.node_id == a.node.node_id
        assert task.name == a.name
        assert task.command == a.command
        assert task.num_attempts == 0
        assert task.max_attempts == a.max_attempts

        # check all job args
        assert a.command == f'OP_NUM_THREADS=1 {sys.executable} ~/runme.py'
