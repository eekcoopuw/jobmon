import os
import pytest
import signal
import subprocess
from time import sleep
from multiprocessing import Process

from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow_run import WorkflowRun
from jobmon.models.workflow_status import WorkflowStatus
from jobmon.client.swarm.executors import sge_utils
from jobmon.client.swarm.workflow.workflow import WorkflowStillRunning

import tests.workflow_utils as wu
from tests.conftest import teardown_db

path_to_file = os.path.dirname(__file__)


def resumable_workflow():
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.workflow import Workflow
    t1 = BashTask("sleep infinity", num_cores=1)
    wfa = "my_simple_dag"
    workflow = Workflow(wfa, project="proj_tools", resume=True)
    workflow.add_tasks([t1])
    return workflow


def run_workflow():
    workflow = resumable_workflow()
    workflow.execute()


def test_resume_running_workflow_fails(db_cfg, env_var):
    teardown_db(db_cfg)
    # create a workflow in a separate process with 1 job that sleeps forever.
    # it must be in a separate process because resume will kill the process
    # that the workflow is running on which would terminate the test process
    p1 = Process(target=run_workflow)
    p1.start()

    # poll till we confirm that job is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        executor_id = None
        max_sleep = 180  # 3 min max till test fails
        slept = 0
        while status != "R" and slept <= max_sleep:
            ji = session.query(JobInstance).one_or_none()
            session.commit()
            sleep(5)
            slept += 5
            if ji:
                status = ji.status
        if ji:
            executor_id = ji.executor_id

    # qdel job if the test timed out
    if slept >= max_sleep and executor_id:
        sge_utils.qdel(executor_id)
        os.kill(p1.pid, signal.SIGKILL)
        p1.terminate()
        teardown_db(db_cfg)
        return

    with pytest.raises(WorkflowStillRunning):
        run_workflow()

    # kill the workflow process and make sure that things get set to stopped
    os.kill(p1.pid, signal.SIGTERM)
    sleep (5)
    os.kill(p1.pid, signal.SIGKILL)

    wf_run_state = subprocess.check_output(f"ps -ax | grep {p1.pid} | "
                                           f"grep -v grep", shell=True,
                                           universal_newlines=True)
    assert "Z" in wf_run_state # make sure workflow run got properly killed
    teardown_db(db_cfg)


def test_cold_resume_workflow(db_cfg, env_var):
    teardown_db(db_cfg)
    p1 = Process(target=run_workflow)
    p1.start()

    # poll till we confirm that job is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        executor_id = None
        max_sleep = 180  # 3 min max till test fails
        slept = 0
        while status != "R" and slept <= max_sleep:
            ji = session.query(JobInstance).one_or_none()
            session.commit()
            sleep(5)
            slept += 5
            if ji:
                status = ji.status
        if ji:
            executor_id = ji.executor_id

    # qdel job if the test timed out
    if slept >= max_sleep and executor_id:
        sge_utils.qdel(executor_id)
        os.kill(p1.pid, signal.SIGKILL)
        os.kill(p1.pid, signal.SIGTERM)
        sleep(5)
        os.kill(p1.pid, signal.SIGKILL)
        teardown_db(db_cfg)
        return

    # need to manually set WFR and WF to stopped because health monitor is not
    # running
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wfr_update = f"""UPDATE workflow_run SET status = '{WorkflowRunStatus.STOPPED}'"""
        wf_update = f"""UPDATE workflow SET status = '{WorkflowStatus.STOPPED}'"""
        DB.session.execute(wfr_update)
        DB.session.execute(wf_update)
        DB.session.commit()
        resp = DB.session.execute("""SELECT status FROM workflow_run""")\
            .fetchall()
        assert resp[0][0] == WorkflowRunStatus.STOPPED

        resp2 = DB.session.execute("""SELECT status FROM workflow""")\
            .fetchall()
        assert resp2[0][0] == WorkflowStatus.STOPPED

    p2 = Process(target=run_workflow)
    p2.start()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        num_wfr = 0
        count = 0
        while num_wfr != 2 and count < 10:
            wfruns = DB.session.query(WorkflowRun).all()
            num_wfr = len(wfruns)
            DB.session.commit()
            count = count + 1
            sleep(5)
        assert len(wfruns) == 2
    wfr_status_query = f"""SELECT status FROM workflow_run"""
    job_instance_query = f"""SELECT status, executor_id FROM job_instance"""

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_run_status = DB.session.execute(wfr_status_query).fetchall()
        assert len(wf_run_status) == 2
        assert wf_run_status[0][0] == WorkflowRunStatus.COLD_RESUME
        assert wf_run_status[1][0] == WorkflowRunStatus.RUNNING

        wf_status = DB.session.execute("""SELECT status FROM workflow""")\
            .fetchall()
        assert len(wf_status) == 1
        assert (wf_status)[0][0] == WorkflowStatus.RUNNING

        job_instances = DB.session.execute(job_instance_query).fetchall()
        count = 5
        while len(job_instances) != 2 and count > 0:
            sleep(5)
            count = count - 1
            DB.session.commit()
            job_instances = DB.session.execute(job_instance_query).fetchall()
        assert len(job_instances) == 2
        assert job_instances[0].status == JobInstanceStatus.KILL_SELF
        ji_state = subprocess.check_output(f'qacct -j '
                                           f'{job_instances[0].executor_id}',
                                           shell=True, universal_newlines=True)
        # make sure that the old job instance is dead
        assert '137' in ji_state.split('exit_status')[1].split('\n')[0]

        job_status = DB.session.execute("""SELECT status FROM job""")\
            .fetchall()
        assert len(job_status) == 1
        if job_status[0][0] == JobStatus.RUNNING:
            subprocess.check_output(f"qdel {job_instances[1].executor_id}",
                                    shell=True, universal_newlines=True)
        else:
            assert job_status[0][0] == JobStatus.INSTANTIATED

    wf_run1_teardown = subprocess.check_output(f"ps -ax | grep {p1.pid} | "
                                               f"grep -v grep", shell=True,
                                               universal_newlines=True)
    assert 'Z+' in wf_run1_teardown # make sure the initial workflow run exited

    os.kill(p1.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p1.pid, signal.SIGKILL)
    os.kill(p2.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p2.pid, signal.SIGKILL)

    teardown_db(db_cfg)


def hot_resumable_workflow():
    from jobmon.client.swarm.workflow.bash_task import BashTask
    from jobmon.client.swarm.workflow.workflow import Workflow
    t1 = BashTask("sleep infinity", num_cores=1)
    wfa = "my_simple_dag"
    workflow = Workflow(wfa, project="proj_tools", resume=True,
                        reset_running_jobs=False)
    workflow.add_tasks([t1])
    return workflow


def run_hot_resumable_workflow():
    workflow = hot_resumable_workflow()
    workflow.execute()


def test_hot_resume_workflow(db_cfg, env_var):
    teardown_db(db_cfg)
    p1 = Process(target=run_hot_resumable_workflow)
    p1.start()

    # poll till we confirm that job is running
    session = db_cfg["DB"].session
    with db_cfg["app"].app_context():
        status = ""
        executor_id = None
        max_sleep = 180  # 3 min max till test fails
        slept = 0
        while status != "R" and slept <= max_sleep:
            ji = session.query(JobInstance).one_or_none()
            session.commit()
            sleep(5)
            slept += 5
            if ji:
                status = ji.status
        if ji:
            executor_id = ji.executor_id

    # qdel job if the test timed out
    if slept >= max_sleep and executor_id:
        sge_utils.qdel(executor_id)
        os.kill(p1.pid, signal.SIGTERM)
        sleep(5)
        os.kill(p1.pid, signal.SIGKILL)
        p1.terminate()
        teardown_db(db_cfg)
        return

    # need to manually set WFR and WF to stopped because health monitor is not
    # running
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wfr_update = f"""UPDATE workflow_run SET status = '{WorkflowRunStatus.STOPPED}'"""
        wf_update = f"""UPDATE workflow SET status = '{WorkflowStatus.STOPPED}'"""
        DB.session.execute(wfr_update)
        DB.session.execute(wf_update)
        DB.session.commit()
        resp = DB.session.execute("""SELECT status FROM workflow_run""") \
            .fetchall()
        assert resp[0][0] == WorkflowRunStatus.STOPPED

        resp2 = DB.session.execute("""SELECT status FROM workflow""") \
            .fetchall()
        assert resp2[0][0] == WorkflowStatus.STOPPED

    p2 = Process(target=run_hot_resumable_workflow)
    p2.start()

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        num_wfr = 0
        count = 0
        while num_wfr != 2 and count < 10:
            wfruns = DB.session.query(WorkflowRun).all()
            num_wfr = len(wfruns)
            DB.session.commit()
            count = count + 1
            sleep(5)
        assert len(wfruns) == 2
    wfr_status_query = f"""SELECT status FROM workflow_run"""
    job_instance_query = f"""SELECT status, executor_id FROM job_instance"""

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        wf_run_status = DB.session.execute(wfr_status_query).fetchall()
        assert len(wf_run_status) == 2
        assert wf_run_status[0][0] == WorkflowRunStatus.HOT_RESUME
        assert wf_run_status[1][0] == WorkflowRunStatus.RUNNING

        wf_status = DB.session.execute("""SELECT status FROM workflow""") \
            .fetchall()
        assert len(wf_status) == 1
        assert (wf_status)[0][0] == WorkflowStatus.RUNNING

        # make sure running job instance did not get stopped and job continued
        # to run
        job_instances = DB.session.execute(job_instance_query).fetchall()
        assert len(job_instances) == 1
        assert job_instances[0].status == JobInstanceStatus.RUNNING

        job_status = DB.session.execute(
            """SELECT status FROM job""").fetchall()
        assert len(job_status) == 1
        assert job_status[0][0] == JobStatus.RUNNING

        # qdel job instance to make sure it gets cleaned up
        subprocess.check_output(f"qdel {job_instances[0].executor_id}",
                                shell=True, universal_newlines=True)

    wf_run1_teardown = subprocess.check_output(f"ps -ax | grep {p1.pid} | "
                                               f"grep -v grep", shell=True,
                                               universal_newlines=True)
    assert 'Z+' in wf_run1_teardown # make sure the initial workflow run exited

    os.kill(p1.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p1.pid, signal.SIGKILL)
    os.kill(p2.pid, signal.SIGTERM)
    sleep(5)
    os.kill(p2.pid, signal.SIGKILL)

    teardown_db(db_cfg)