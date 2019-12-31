

def test_scheduler_boot_shutdown(dummy_scheduler):
    dummy_scheduler.start()
    assert dummy_scheduler.executor.started

    # TODO: add to model to register an executor

    dummy_scheduler.stop()
    assert not dummy_scheduler.executor.started
    assert dummy_scheduler._stop_event.is_set()
