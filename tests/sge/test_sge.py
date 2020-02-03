
def test_reconciler_sge(db_cfg, jlm_sge_daemon):

    # Queue a job
    task = Task(command=sge.true_path(f"{path_to_file}/shellfiles/sleep.sh"),
                name="sleepyjob_pass", num_cores=1)

    job = jlm_sge_daemon.bind_task(task)
    jlm_sge_daemon.adjust_resources_and_queue(job)

    # Give the job_state_manager some time to process the error message
    # This test job just sleeps for 60s, so it should not be missing
    # DO NOT put in a while-True loop
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    app.app_context().push()
    sql = "SELECT status FROM job_instance"
    # Test should not fail in the first 60 seconds
    for i in range(0, 30):
        res = DB.session.execute(sql).fetchone()
        DB.session.commit()
        jir = jlm_sge_daemon.job_inst_reconciler
        jir.reconcile()
        jlm_sge_daemon._sync()
        if res is not None:
            assert res[0] != "E"
        assert len(jlm_sge_daemon.all_error) == 0
        sleep(2)



def test_reconciler_sge_new_heartbeats(jlm_sge_daemon, db_cfg):
    jir = jlm_sge_daemon.job_inst_reconciler
    jif = jlm_sge_daemon.job_instance_factory

    task = BashTask(command="sleep 5", name="heartbeat_sleeper", num_cores=1,
                    max_runtime_seconds=500)
    job = jlm_sge_daemon.bind_task(task)
    jlm_sge_daemon.adjust_resources_and_queue(job)

    jif.instantiate_queued_jobs()
    jir.reconcile()
    jlm_sge_daemon._sync()
    count = 0
    while len(jlm_sge_daemon.all_done) < 1 and count < 10:
        sleep(50)
        jir.reconcile()
        jlm_sge_daemon.last_sync = None
        jlm_sge_daemon._sync()
        count += 1
    assert jlm_sge_daemon.all_done
    job_id = jlm_sge_daemon.all_done.pop().job_id
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        query = """
        SELECT submitted_date, report_by_date
        FROM job_instance
        WHERE job_id = {}""".format(job_id)
        res = DB.session.execute(query).fetchone()
        DB.session.commit()
    start, end = res
    assert start < end  # indicating at least one heartbeat got logged




def test_reconciler_sge_timeout(jlm_sge_daemon, db_cfg):
    # Flush the error queue to avoid false positives from other tests
    jlm_sge_daemon.all_error = set()

    # Queue a test job
    task = Task(command=sge.true_path(f"{path_to_file}/shellfiles/sleep.sh"),
                name="sleepyjob_fail", max_attempts=3, max_runtime_seconds=3,
                num_cores=1)
    job = jlm_sge_daemon.bind_task(task)
    jlm_sge_daemon.adjust_resources_and_queue(job)

    # Give the SGE scheduler some time to get the job scheduled and for the
    # reconciliation daemon to kill the job.
    # The sleepy job tries to sleep for 60 seconds, but times out after 3
    # seconds (well, when the reconciler runs, typically every 10 seconds)
    # 60
    timeout_and_skip(20, 200, 1, "sleepyjob_fail", partial(
        reconciler_sge_timeout_check,
        job_list_manager_reconciliation=jlm_sge_daemon,
        dag_id=jlm_sge_daemon.dag_id,
        job_id=job.job_id,
        db_cfg=db_cfg))


def reconciler_sge_timeout_check(job_list_manager_reconciliation, dag_id,
                                 job_id, db_cfg):
    jlm = job_list_manager_reconciliation
    jobs = jlm.get_job_statuses()
    completed, failed, adjusting = jlm.parse_adjusting_done_and_errors(jobs)
    if adjusting:
        for task in adjusting:
            task.executor_parameters = partial(jlm.adjust_resources,
                                               task)  # change callable
            jlm.adjust_resources_and_queue(task)
    if len(job_list_manager_reconciliation.all_error) == 1:
        assert job_id in [
            j.job_id for j in job_list_manager_reconciliation.all_error]

        # The job should have been tried 3 times...
        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            query = f"select num_attempts from job where job_id = {job_id}"
            res = DB.session.execute(query).fetchone()
            DB.session.commit()
        assert res[0] == 3
        return True
    else:
        return False




# def test_ignore_qw_in_timeouts(jlm_sge_daemon, db_cfg):
#     # Qsub a long running job -> queue another job that waits on it,
#     # to simulate a hqw -> set the timeout for that hqw job to something
#     # short... make sure that job doesn't actually get killed
#     # TBD I don't think that has been implemented.
#     task = Task(command=sge.true_path(f"{path_to_file}/shellfiles/sleep.sh"),
#                 name="sleepyjob", max_attempts=3, max_runtime_seconds=3,
#                 num_cores=1)
#     job = jlm_sge_daemon.bind_task(task)
#     jlm_sge_daemon.adjust_resources_and_queue(job)

#     # Give the SGE scheduler some time to get the job scheduled and for the
#     # reconciliation daemon to kill the job
#     # The sleepy job tries to sleep for 60 seconds, but times out after 3
#     # seconds

#     timeout_and_skip(10, 200, 1, "sleepyjob", partial(
#         reconciler_sge_timeout_check,
#         job_list_manager_reconciliation=jlm_sge_daemon,
#         dag_id=jlm_sge_daemon.dag_id,
#         job_id=job.job_id,
#         db_cfg=db_cfg))


# def test_reconciler_sge_dag_heartbeats(jlm_sge_daemon, db_cfg):
#     dag_id = jlm_sge_daemon.dag_id
#     jir = jlm_sge_daemon.job_inst_reconciler
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]
#     with app.app_context():
#         query = """
#         SELECT heartbeat_date
#         FROM task_dag
#         WHERE dag_id = {}""".format(dag_id)
#         res = DB.session.execute(query).fetchone()
#         DB.session.commit()
#         start = res
#     # Sleep to ensure that the timestamps will be different
#     sleep(2)
#     jir.reconcile()
#     app = db_cfg["app"]
#     DB = db_cfg["DB"]
#     with app.app_context():
#         query = """
#         SELECT heartbeat_date
#         FROM task_dag
#         WHERE dag_id = {}""".format(dag_id)
#         res = DB.session.execute(query).fetchone()
#         DB.session.commit()
#         end = res
#     assert start[0] < end[0]


# def test_sge_valid_command(jlm_sge_no_daemon):
#     job = jlm_sge_no_daemon.bind_task(
#         Task(command="ls", name="sgefbb", num_cores=3,
#              max_runtime_seconds='1000', m_mem_free='600M'))
#     jlm_sge_no_daemon.adjust_resources_and_queue(job)
#     jlm_sge_no_daemon.job_instance_factory.instantiate_queued_jobs()
#     jlm_sge_no_daemon._sync()
#     assert (jlm_sge_no_daemon.bound_tasks[job.job_id].status ==
#             JobStatus.INSTANTIATED)
#     print("finishing test_sge_valid_command")


# def test_ji_unknown_state(jlm_sge_no_daemon, db_cfg):
#     """should try to log a report by date after being set to the L state and
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

#     jlm = jlm_sge_no_daemon
#     jif = jlm.job_instance_factory
#     job = jlm.bind_task(Task(command="sleep 60", name="lost_task",
#                              num_cores=3, max_runtime_seconds='70',
#                              m_mem_free='600M'))
#     jlm.adjust_resources_and_queue(job)
#     jids = jif.instantiate_queued_jobs()
#     jlm._sync()
#     resp = query_till_running(db_cfg)
#     while resp.status != 'R':
#         resp = query_till_running(db_cfg)
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
