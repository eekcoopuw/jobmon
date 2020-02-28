import pytest
import logging
from subprocess import check_output
import time

from unittest import mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# import jobmon.client.swarm.executors.sge
from tenacity import stop_after_attempt
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
<<<<<<< HEAD
# from jobmon.models.job_instance_status import JobInstanceStatus
# from jobmon.models.job_instance import JobInstance
from jobmon.client.job_management.job_list_manager import JobListManager
from jobmon.client.workflow.executable_task import ExecutableTask
from jobmon.execution.scheduler.job_instance_scheduler import \
    JobInstanceScheduler
from jobmon.execution.scheduler.executor_job import ExecutorJob

=======
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.job_instance import JobInstance
from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.client.swarm.workflow.executable_task import ExecutableTask
from jobmon.client import shared_requester as req
from jobmon.models.workflow import Workflow
from jobmon.client.swarm.job_management.swarm_job import SwarmJob
from jobmon.serializers import SerializeExecutorJobInstance
from tests.conftest import teardown_db

from tests.conftest import teardown_db
>>>>>>> d8544f7b2444c25a98a75878093647681596e6bb

class Task(ExecutableTask):
    """Test version of the Task class for use in this module"""

    def __init__(self, command, name, *args, **kwargs):
        super(Task, self).__init__(command=command, name=name, max_attempts=1,
                                   *args, **kwargs)


class SingleDagScheduler(JobInstanceScheduler):

    def __init__(self, executor, config, url, dag_id, *args, **kwargs):
        self._time = '2010-01-01 00:00:00'
        self._Session = sessionmaker(bind=create_engine(url))
        self._dag_id = dag_id
        super().__init__(executor, config)

    def _get_jobs_queued_for_instantiation(self):
        session = self._Session()
        try:
            new_time = session.execute(
                "select UTC_TIMESTAMP as time").fetchone()['time']
            session.commit()
            new_time = time.strftime("%Y-%m-%d %H:%M:%S")
            query_time = self._time
            self._time = new_time

            query = """
                SELECT
                    job.*
                FROM
                    job
                WHERE
                    job.dag_id = :dag_id
                    AND job.status = :job_status
                    AND job.status_date >= :last_sync
                ORDER BY job.job_id
                LIMIT :n_queued_jobs"""
            jobs = session.query(Job).from_statement(text(query)).params(
                dag_id=self._dag_id,
                job_status=JobStatus.QUEUED_FOR_INSTANTIATION,
                last_sync=query_time,
                n_queued_jobs=int(self.config.n_queued)
            ).all()
            session.commit()
            jobs = [j.to_wire_as_executor_job() for j in jobs]
            jobs = jobs = [
                ExecutorJob.from_wire(j, self.executor.__class__.__name__,
                                      self.requester)
                for j in jobs]
        finally:
            session.close()
        return jobs


def sequential_scheduler_instance(host, port, reconciliation_interval,
                                  heartbeat_interval, report_by_buffer, url,
                                  dag_id):
    from jobmon.execution.scheduler.execution_config import ExecutionConfig
    from jobmon.execution.strategies.sequential import SequentialExecutor

    cfg = ExecutionConfig.from_defaults()
    cfg.host = host
    cfg.port = port
    cfg.reconciliation_interval = reconciliation_interval
    cfg.heartbeat_interval = heartbeat_interval
    cfg.report_by_buffer = report_by_buffer

    executor = SequentialExecutor()
    return SingleDagScheduler(executor, cfg, url, dag_id)


@pytest.fixture(scope='function')
def sequential_scheduler_jlm(real_jsm_jqs, monkeypatch, db_cfg, real_dag_id,
                             client_env):
    # modify global config before importing anything else
    from jobmon import config
    config.jobmon_server_sqdn = real_jsm_jqs["JOBMON_HOST"]
    config.jobmon_service_port = real_jsm_jqs["JOBMON_PORT"]

    # build sequential scheduler
    scheduler = sequential_scheduler_instance(
        real_jsm_jqs["JOBMON_HOST"], real_jsm_jqs["JOBMON_PORT"], 4, 2, 2.1,
        db_cfg["server_config"].conn_str, real_dag_id)

    yield scheduler, JobListManager(real_dag_id)
    scheduler.stop()
    config.jobmon_server_sqdn = None
    config.jobmon_service_port = None


def run_sequential_scheduler_instance(host, port, reconciliation_interval,
                                      heartbeat_interval, report_by_buffer,
                                      url, dag_id):
    scheduler = sequential_scheduler_instance(
        host, port, reconciliation_interval, heartbeat_interval,
        report_by_buffer)
    scheduler.run()

<<<<<<< HEAD

@pytest.fixture(scope='function')
def sequential_scheduler_process_jlm(real_jsm_jqs, monkeypatch, db_cfg,
                                     real_dag_id, client_env):
    import multiprocessing as mp
    import time

    monkeypatch.setenv("JOBMON_SERVER_SQDN", real_jsm_jqs["JOBMON_HOST"])
    monkeypatch.setenv("JOBMON_SERVICE_PORT", real_jsm_jqs["JOBMON_PORT"])

    ctx = mp.get_context('spawn')
    p1 = ctx.Process(target=run_sequential_scheduler_instance,
                     args=(real_jsm_jqs["JOBMON_HOST"],
                           real_jsm_jqs["JOBMON_PORT"],
                           4, 2, 2.1, db_cfg["server_config"].conn_str,
                           real_dag_id))
    p1.start()
    time.sleep(10)
    yield JobListManager(real_dag_id)
    p1.terminate()


def test_sync(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    now = jlm.last_sync
=======
def test_sync(db_cfg, jlm_sge_no_daemon):
    teardown_db(db_cfg)
    now = jlm_sge_no_daemon.last_sync
>>>>>>> d8544f7b2444c25a98a75878093647681596e6bb
    assert now is not None

    # This job will intentionally fail
    job = jlm.bind_task(
        Task(command='fizzbuzz', name='bar',
<<<<<<< HEAD
             executor_class="SequentialExecutor"))
=======
             m_mem_free='1G',
             max_runtime_seconds=1000,
             num_cores=1))
>>>>>>> d8544f7b2444c25a98a75878093647681596e6bb
    # create job instances
    jlm.adjust_resources_and_queue(job)
    jid = scheduler.instantiate_queued_jobs()
    assert jid

    # with a new job failed, make sure that the sync has been updated and the
    # call with the sync filter actually returns jobs. must wait at least 1
    # second for the times to be different
    time.sleep(1)
    jlm._sync()
    new_now = jlm.last_sync
    assert new_now > now
<<<<<<< HEAD
    assert len(jlm.all_error) > 0
=======
    assert len(jlm_sge_no_daemon.all_error) > 0
    teardown_db(db_cfg)


def test_invalid_command(job_list_manager):
    job = job_list_manager.bind_task(Task(command='foo', name='bar',
                                          num_cores=1))
    njobs0 = job_list_manager.active_jobs
    assert len(njobs0) == 0

    job_list_manager.adjust_resources_and_queue(job)
    njobs1 = job_list_manager.active_jobs
    assert len(njobs1) == 1
    assert len(job_list_manager.all_error) == 0
    job_list_manager.job_instance_factory.instantiate_queued_jobs()

    # sleep is okay because we are using the sequential executor which should
    # always schedule the job
    sleep(35)
    job_list_manager._sync()
    assert len(job_list_manager.all_error) > 0

>>>>>>> d8544f7b2444c25a98a75878093647681596e6bb


def test_invalid_command(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    job = jlm.bind_task(Task(command='foo', name='bar',
                             executor_class="SequentialExecutor"))
    assert len(jlm.active_jobs) == 0

    jlm.adjust_resources_and_queue(job)
    assert len(jlm.active_jobs) == 1
    assert len(jlm.all_error) == 0

    scheduler.instantiate_queued_jobs()
    jlm._sync()
    assert len(jlm.all_error) > 0


def test_valid_command(sequential_scheduler_jlm):
    scheduler, jlm = sequential_scheduler_jlm
    job = jlm.bind_task(Task(command='ls', name='baz',
                             executor_class="SequentialExecutor"))
    assert len(jlm.active_jobs) == 0
    assert len(jlm.all_done) == 0

    jlm.adjust_resources_and_queue(job)
    assert len(jlm.active_jobs) == 1

    scheduler.instantiate_queued_jobs()
    jlm._sync()
    assert len(jlm.all_done) > 0


def test_blocking_update_timeout(client_env, real_dag_id):
    jlm = JobListManager(real_dag_id)
    job = jlm.bind_task(
        Task(command="sleep 3", name="foobarbaz",
             executor_class="SequentialExecutor"))
    jlm.adjust_resources_and_queue(job)

    with pytest.raises(RuntimeError) as error:
        jlm.block_until_any_done_or_error(timeout=2)

    expected_msg = ("Not all tasks completed within the given workflow "
                    "timeout length (2 seconds). Submitted tasks will still"
                    " run, but the workflow will need to be restarted.")
    assert expected_msg == str(error.value)


def test_sge_valid_command(jlm_sge_no_daemon):
    job = jlm_sge_no_daemon.bind_task(
        Task(command="ls", name="sgefbb", num_cores=3,
             max_runtime_seconds=1000, m_mem_free='600M'))
    jlm_sge_no_daemon.adjust_resources_and_queue(job)
    jlm_sge_no_daemon.job_instance_factory.instantiate_queued_jobs()
    jlm_sge_no_daemon._sync()
    assert (jlm_sge_no_daemon.bound_tasks[job.job_id].status ==
            JobStatus.INSTANTIATED)
    print("finishing test_sge_valid_command")



# def test_ji_unknown_state(sequential_scheduler_process_jlm, db_cfg):
#     """should try to log a report by date after being set to the U state and
#     fail"""
#     def query_till_running(db_cfg):
#         app = db_cfg["app"]
#         DB = db_cfg["DB"]
#         with app.app_context():
#             resp = DB.session.execute(
#                 """SELECT status, executor_id FROM job_instance"""
#             ).fetchall()[-1]
#             DB.session.commit()
#         return resp

#     jlm = sequential_scheduler_jlm
#     job = jlm.bind_task(Task(command="sleep 60", name="lost_task",
# <<<<<<< HEAD
#                              executor_class="SequentialExecutor"))
# =======
#                              num_cores=1, max_runtime_seconds=120,
#                              m_mem_free='600M'))
# >>>>>>> 26f460d6fac5f19cf5931f181e42e83fca21d0ec
#     jlm.adjust_resources_and_queue(job)
#     jlm._sync()
#     resp = query_till_running(db_cfg)
# <<<<<<< HEAD

#     while resp.status != 'R':
# =======
#     count = 0
#     while resp.status != 'R' and count < 20:
# >>>>>>> 26f460d6fac5f19cf5931f181e42e83fca21d0ec
#         resp = query_till_running(db_cfg)
#         sleep(10)
#         count = count + 1
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]
#     with app.app_context():
#         DB.session.execute("""
#             UPDATE job_instance
#             SET status = 'U'
#             WHERE job_instance_id = {}""".format(jids[0]))
#         DB.session.commit()
#     exec_id = resp.executor_id
#     exit_status = None
#     tries = 1
#     while exit_status is None and tries < 10:
#         try:
#             exit_status = check_output(
#                 f"qacct -j {exec_id} | grep exit_status",
#                 shell=True, universal_newlines=True)
#         except Exception:
#             tries += 1
#             sleep(3)
#     # 9 indicates sigkill signal was sent as expected
#     assert '9' in exit_status
=======
            job_list_manager.get_job_statuses()


class ExpectedException(Exception):
    pass


def mock_qsub_error(cmd, shell=False, universal_newlines=True):
    raise ExpectedException("qsub didnt work")


def mock_parse_qsub_resp_error(cmd, shell=False, universal_newlines=True):
    return ("NO executor id here")


def test_job_instance_qsub_error(db_cfg, jlm_sge_no_daemon, monkeypatch,
                                 caplog):
    teardown_db(db_cfg)
    monkeypatch.setattr(jobmon.client.swarm.executors.sge,
                        "check_output", mock_qsub_error)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="ls", name="sgefbb", num_cores=3,
                             max_runtime_seconds=1000, m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jif.instantiate_queued_jobs()
    jlm._sync()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute("""SELECT * FROM job_instance""").fetchall()
        DB.session.commit()

    # most recent job is from this test
    assert resp[-1].status == 'W'
    assert resp[-1].executor_id is None
    assert "Received -99999 meaning the job did not qsub properly, moving " \
           "to 'W' state" in caplog.text
    teardown_db(db_cfg)


def test_job_instance_bad_qsub_parse(db_cfg, jlm_sge_no_daemon, monkeypatch,
                                     caplog):
    teardown_db(db_cfg)
    monkeypatch.setattr(jobmon.client.swarm.executors.sge,
                        "check_output", mock_parse_qsub_resp_error)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="ls", name="sgefbb", num_cores=3,
                             max_runtime_seconds=1000, m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jif.instantiate_queued_jobs()
    jlm._sync()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute("""SELECT * FROM job_instance""").fetchall()
        job_info = DB.session.execute("""SELECT * FROM job""").fetchall()
        DB.session.commit()
    assert resp[-1].status == 'W'
    assert resp[-1].executor_id is None
    assert job_info[-1].status == 'F'
    assert "Got response from qsub but did not contain a valid executor_id. " \
           "Using (-33333), and moving to 'W' state" in caplog.text
    teardown_db(db_cfg)


def query_till_running(db_cfg):
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        resp = DB.session.execute(
            """SELECT status, executor_id FROM job_instance"""
        ).fetchall()[-1]
        DB.session.commit()
    return resp


def test_ji_unknown_state(db_cfg, jlm_sge_no_daemon):
    """should try to log a report by date after being set to the L state and
    fail"""
    teardown_db(db_cfg)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="sleep 60", name="lost_task",
                             num_cores=1, max_runtime_seconds=80,
                             m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jids = jif.instantiate_queued_jobs()
    jlm._sync()
    resp = query_till_running(db_cfg)
    count = 0
    while resp.status != 'R' and count < 20:
        resp = query_till_running(db_cfg)
        sleep(10)
        count = count + 1
    assert resp.status == 'R', "Job never entered running state"
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job_instance
            SET status = 'U'
            WHERE job_instance_id = {}""".format(jids[0]))
        DB.session.commit()
    exec_id = resp.executor_id
    exit_status = None
    tries = 1
    while exit_status is None and tries < 10:
        try:
            exit_status = check_output(
                f"qacct -j {exec_id} | grep exit_status",
                shell=True, universal_newlines=True)
        except Exception:
            tries += 1
            sleep(3)
    # 9 indicates sigkill signal was sent as expected
    assert '9' in exit_status
    teardown_db(db_cfg)


def test_ji_kill_self_state(db_cfg, jlm_sge_no_daemon):
    """Job instance should poll for for a job state KILL_SELF and kill
    itself. Basically test_ji_unknown_state but setting K state instead
    """
    teardown_db(db_cfg)
    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory
    job = jlm.bind_task(Task(command="sleep 60", name="kill_self_task",
                             num_cores=1, max_runtime_seconds=120,
                             m_mem_free='600M'))
    jlm.adjust_resources_and_queue(job)
    jids = jif.instantiate_queued_jobs()
    jlm._sync()
    resp = query_till_running(db_cfg)
    count = 0
    while resp.status != 'R' and count < 20:
        resp = query_till_running(db_cfg)
        sleep(10)
        count = count + 1
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        DB.session.execute("""
            UPDATE job_instance
            SET status = 'K'
            WHERE job_instance_id = {}""".format(jids[0]))
        DB.session.commit()
    exec_id = resp.executor_id
    exit_status = None
    tries = 1
    while exit_status is None and tries < 10:
        try:
            exit_status = check_output(
                f"qacct -j {exec_id} | grep exit_status",
                shell=True, universal_newlines=True)
        except Exception:
            tries += 1
            sleep(3)
    # 9 indicates sigkill signal was sent as expected
    assert '9' in exit_status
    teardown_db(db_cfg)


def test_context_args(db_cfg, jlm_sge_no_daemon, caplog):
    teardown_db(db_cfg)
    caplog.set_level(logging.DEBUG)

    jlm = jlm_sge_no_daemon
    jif = jlm.job_instance_factory

    job = jlm.bind_task(
        Task(command="sge_foobar",
             name="test_context_args", num_cores=2, m_mem_free='4G',
             max_runtime_seconds=1000,
             context_args={'sge_add_args': '-a foo'}))

    jlm.adjust_resources_and_queue(job)
    jif.instantiate_queued_jobs()

    assert "-a foo" in caplog.text
    teardown_db(db_cfg)


def test_set_kill_self_state(real_dag_id, db_cfg):
    teardown_db(db_cfg)
    # Create workflow
    rc, response = req.send_request(
        app_route='/workflow',
        message={'dag_id': real_dag_id,
                 'workflow_args': 'test_dup_args',
                 'workflow_hash': '123fe34gr',
                 'name': 'dup_args',
                 'description': '',
                 'user': 'user'},
        request_type='post')
    wf = Workflow.from_wire(response['workflow_dct'])

    # Create workflow_run
    rc, response = req.send_request(
        app_route='/workflow_run',
        message={'workflow_id': wf.id,
                 'user': 'user',
                 'hostname': 'test.host.ihme.washington.edu',
                 'pid': 123,
                 'stderr': '/',
                 'stdout': '/',
                 'project': 'proj_tools',
                 'slack_channel': '',
                 'executor_class': 'SGEExecutor',
                 'working_dir': '/'},
        request_type='post')
    wfr_id = response['workflow_run_id']

    # Create a job and add it to workflow run
    _, response = req.send_request(
        app_route='/job',
        message={'name': 'bar',
                 'job_hash': 12334,
                 'command': 'baz',
                 'dag_id': str(real_dag_id)},
        request_type='post')
    swarm_job = SwarmJob.from_wire(response['job_dct'])

    # Queue job for valid state transition
    req.send_request(
        app_route='/job/{}/queue'.format(swarm_job.job_id),
        message={},
        request_type='post')

    # Create first of three job_instances
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')

    job_instance_id = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    # Change status of first job_instance to "R"
    with app.app_context():
        query = """UPDATE job_instance
                   SET status="R"
                   WHERE job_instance_id = {job_instance_id}
                   """.format(job_instance_id=job_instance_id)
        DB.session.execute(query)
        DB.session.commit()

    # Create 2 of 3 job_instances
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')

    job_instance_id_2 = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

    # Update second job_instance status to "B"
    with app.app_context():
        query = """UPDATE job_instance
                   SET status="B"
                   WHERE job_instance_id = {job_instance_id}
                   """.format(job_instance_id=job_instance_id_2)
        DB.session.execute(query)
        DB.session.commit()

    # Create third job_instance
    _, response = req.send_request(
        app_route='/job_instance',
        message={'job_id': str(swarm_job.job_id),
                 'executor_type': 'dummy_exec'},
        request_type='post')

    job_instance_id_3 = SerializeExecutorJobInstance.kwargs_from_wire(
        response['job_instance'])["job_instance_id"]

    # Update third job_instance status to "I"
    with app.app_context():
        query = """UPDATE job_instance
                   SET status="I"
                   WHERE job_instance_id = {job_instance_id}
                   """.format(job_instance_id=job_instance_id_3)
        DB.session.execute(query)
        DB.session.commit()

    # Change all states to "K"
    req.send_request(
        app_route='/job_instance/{}/nonterminal_to_k_status'.format(wfr_id),
        message={},
        request_type='post')

    # Query for all job_instance that have K state, assert that it's 3
    with app.app_context():
        query = """SELECT COUNT(*)
                   FROM job_instance
                   WHERE workflow_run_id = {workflow_run_id}
                   AND status = "K"
                   """.format(workflow_run_id=wfr_id)
        k_status_count = DB.session.execute(query).fetchone()
        DB.session.commit()

    assert k_status_count[0] == 3
    teardown_db(db_cfg)
>>>>>>> d8544f7b2444c25a98a75878093647681596e6bb
