import os
from time import sleep
from subprocess import check_output


def match_name_to_sge_name(jid):
    # Try this a couple of times... SGE is weird
    retries = 10
    while retries > 0:
        try:
            sge_jobname = check_output(
                "qacct -j {} | grep jobname".format(jid),
                shell=True).decode()
            break
        except Exception:
            try:
                sge_jobname = check_output(
                    "qstat -j {} | grep job_name".format(jid),
                    shell=True).decode()
                break
            except Exception:
                pass
            sleep(10 - retries)
            retries = retries - 1
            if retries == 0:
                raise RuntimeError("Attempted to use qstat to get jobname. "
                                   "Giving up after {} "
                                   "retries".format(retries))
    sge_jobname = sge_jobname.split()[-1].strip()
    return sge_jobname


def test_exceed_runtime_task(db_cfg, client_env):
    """Tests that when a task exceeds the requested amount of run time on SGE, it
    succcessfully gets killed"""
    from subprocess import check_output
    from jobmon.client.api import BashTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.execution.strategies.base import ExecutorParameters
    from jobmon.models.task import Task

    name = "over_run_time_task"
    executor_parameters = ExecutorParameters(m_mem_free='1G', max_runtime_seconds=5,
                                             num_cores=1, queue='all.q',
                                             executor_class="SGElExecutor")
    task = BashTask(command="sleep 10", name=name, executor_parameters=executor_parameters,
                    executor_class="SGEExecutor", max_attempts=1)
    workflow = UnknownWorkflow(workflow_args="over_run_time_task_args",
                               executor_class="SGEExecutor")
    workflow.add_tasks([task])
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task = DB.session.query(Task).filter_by(name=name).first()
        tid = [ti for ti in task.task_instances][0].executor_id
        resp = check_output(f"qacct -j {tid} | grep 'exit_status\|failed'",
                            shell=True, universal_newlines=True)
        assert ("247" in resp) or ("137" in resp)
        assert task.task_instances[0].status == 'Z'
        assert task.status == 'F'

    import pdb
    pdb.set_trace()
    sge_jobname = match_name_to_sge_name(tid)
    assert sge_jobname == name


def test_exceed_mem_task(db_cfg, client_env):
    """Tests that when a task exceeds the requested amount of memory on SGE, it
    succcessfully gets killed"""
    from subprocess import check_output
    from jobmon.client.api import PythonTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.execution.strategies.base import ExecutorParameters
    from jobmon.models.task import Task

    name = "exeeded_requested_memory_test"
    this_file = os.path.dirname(__file__)
    exceed_mem_path = os.path.abspath(os.path.expanduser(
        f"{this_file}/../_scripts/exceed_mem.py"))

    executor_parameters = ExecutorParameters(m_mem_free='130M', max_runtime_seconds=40,
                                             num_cores=1, queue='all.q',
                                             executor_class="SGEExecutor")
    task = PythonTask(script=exceed_mem_path, name=name, executor_parameters=executor_parameters,
                    executor_class="SGEExecutor", max_attempts=1)
    workflow = UnknownWorkflow(workflow_args="over_memory_task_args",
                               executor_class="SGEExecutor")
    workflow.add_tasks([task])
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task = DB.session.query(Task).filter_by(name=name).first()
        tid = [ti for ti in task.task_instances][0].executor_id
        resp = check_output(f"qacct -j {tid} | grep 'exit_status\|failed'",
                            shell=True, universal_newlines=True)
        assert ("247" in resp) or ("137" in resp)
        assert task.task_instances[0].status == 'Z'
        assert task.status == 'F'

    sge_jobname = match_name_to_sge_name(tid)
    assert sge_jobname == name


def test_under_request_memory_then_scale(db_cfg, client_env):
    from subprocess import check_output
    from jobmon.client.api import PythonTask
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.execution.strategies.base import ExecutorParameters
    from jobmon.client.execution.strategies.sge.sge_parameters import SGEParameters
    from jobmon.models.task import Task

    name = "exeeded_requested_memory_scaling_test"
    this_file = os.path.dirname(__file__)
    exceed_mem_path = os.path.abspath(os.path.expanduser(
        f"{this_file}/../_scripts/exceed_mem.py"))

    executor_parameters = ExecutorParameters(m_mem_free='600M', max_runtime_seconds=40,
                                             num_cores=1, queue='all.q',
                                             executor_class="SGEExecutor")

    # executor_parameters = SGEParameters(m_mem_free='600M', max_runtime_seconds=40,
    #                                          num_cores=1, queue='all.q')
    task = PythonTask(script=exceed_mem_path, name=name,
                      executor_parameters=executor_parameters,
                      executor_class="SGEExecutor", max_attempts=2)
    import pdb
    pdb.set_trace()
    workflow = UnknownWorkflow(workflow_args="over_memory_scaling_task_args",
                               executor_class="SGEExecutor")
    workflow.add_tasks([task])
    workflow.run()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        task = DB.session.query(Task).filter_by(name=name).first()
        tid = [ti for ti in task.task_instances][0].executor_id
        resp = check_output(f"qacct -j {tid} | grep 'exit_status\|failed'",
                            shell=True, universal_newlines=True)
        import pdb
        pdb.set_trace()
        assert ("247" in resp) or ("137" in resp)
        assert task.task_instances[0].status == 'Z'
        assert task.task_instances[1].status == 'Z'
        assert task.status == 'F'
    sge_jobname = match_name_to_sge_name(tid)
    assert sge_jobname == name

