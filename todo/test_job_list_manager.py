

class Task(ExecutableTask):
    """Test version of the Task class for use in this module"""

    def __init__(self, command, name, *args, **kwargs):
        super(Task, self).__init__(command=command, name=name, max_attempts=1,
                                   *args, **kwargs)


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
