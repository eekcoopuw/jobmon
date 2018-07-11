from jobmon.executors import Executor


class DummyExecutor(Executor):

    def execute(job_instance):
        import random
        # qsub
        executor_id = random.randint(1, 1e7)
        return executor_id
