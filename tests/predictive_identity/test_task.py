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


# def test_bash_task_args(db_cfg, env_var):
#     from jobmon.client.workflow.templates.bash_task import BashTask
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]

#     a = BashTask(command="echo 'Hello Jobmon'", num_cores=1, m_mem_free='2G',
#                  max_attempts=1)
#     a.bind()

#     with app.app_context():
#         job = DB.session.query(Job).filter_by(job_id=job_id).all()
#         num_cores = job[0].executor_parameter_set.num_cores
#         m_mem_free = job[0].executor_parameter_set.m_mem_free
#         max_attempts = job[0].max_attempts
#         DB.session.commit()
#         # check all job args
#         assert num_cores == 1
#         assert m_mem_free == 2
#         assert max_attempts == 1


# def test_python_task_equality():
#     a = PythonTask(script='~/runme.py', args=[1])
#     a_again = PythonTask(script='~/runme.py', args=[1])
#     assert a == a_again

#     b = PythonTask(script='~/runme.py', args=[2], upstream_tasks=[a, a_again])
#     assert b != a
#     assert len(b.upstream_tasks) == 1


# def test_python_task_args(db_cfg, jlm_sge_no_daemon):
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]
#     a = PythonTask(script='~/runme.py', env_variables={'OP_NUM_THREADS': 1},
#                    num_cores=1, m_mem_free='2G', max_attempts=1)
#     job = jlm_sge_no_daemon.bind_task(a)
#     job_id = job.job_id
#     jlm_sge_no_daemon.adjust_resources_and_queue(job)

#     with app.app_context():
#         job = DB.session.query(Job).filter_by(job_id=job_id).all()
#         command = job[0].command
#         num_cores = job[0].executor_parameter_set.num_cores
#         m_mem_free = job[0].executor_parameter_set.m_mem_free
#         max_attempts = job[0].max_attempts
#         DB.session.commit()
#         # check all job args
#         assert command == 'OP_NUM_THREADS=1 {} ~/runme.py'.format(
#             sys.executable)
#         assert num_cores == 1
#         assert m_mem_free == 2
#         assert max_attempts == 1
