from jobmon.client import BashTask
from jobmon.client import Workflow
from jobmon.client.swarm.executors import dummy
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.client.worker_node.execution_wrapper import parse_arguments
from tests.conftest import teardown_db


def test_wedged_dag(monkeypatch, env_var, db_cfg):
    teardown_db(db_cfg)

    class MockDummyExecutor(
            dummy.DummyExecutor):

        def execute(self, command: str, name: str,
                    executor_parameters) -> int:
            command = " ".join(command.split()[1:])
            kwargs = parse_arguments(command)

            if kwargs["job_instance_id"] == 1:
                job_inst_query = """
                UPDATE job_instance
                SET status = 'D'
                WHERE job_instance_id = {job_instance_id}
                """.format(job_instance_id=kwargs["job_instance_id"])
                job_query = """
                UPDATE job
                JOIN job_instance ON job.job_id = job_instance.job_id
                SET job.status = 'D',
                    job.status_date = SUBTIME(UTC_TIMESTAMP(),
                                              SEC_TO_TIME(600))
                WHERE job_instance_id = {job_instance_id}
                """.format(job_instance_id=kwargs["job_instance_id"])
            else:
                job_inst_query = """
                UPDATE job_instance
                SET status = 'D',
                    status_date = UTC_TIMESTAMP()
                WHERE job_instance_id = {job_instance_id}
                """.format(job_instance_id=kwargs["job_instance_id"])
                job_query = """
                UPDATE job
                JOIN job_instance ON job.job_id = job_instance.job_id
                SET job.status = 'D',
                    job.status_date = UTC_TIMESTAMP()
                WHERE job_instance_id = {job_instance_id}
                """.format(job_instance_id=kwargs["job_instance_id"])

            app = db_cfg["app"]
            DB = db_cfg["DB"]
            with app.app_context():
                DB.session.execute(job_inst_query)
                DB.session.commit()
                DB.session.execute(job_query)
                DB.session.commit()
            return super().execute(command, name, executor_parameters)

    monkeypatch.setattr(dummy, "DummyExecutor", MockDummyExecutor)

    t1 = BashTask("sleep 10", executor_class="DummyExecutor",
                  max_runtime_seconds=1)
    t2 = BashTask("sleep 5", upstream_tasks=[t1],
                  executor_class="DummyExecutor",
                  max_runtime_seconds=2)
    workflow = Workflow(executor_class="DummyExecutor",
                        seconds_until_timeout=700)
    workflow.add_tasks([t1, t2])
    status = workflow.run()

    assert status == DagExecutionStatus.SUCCEEDED

    teardown_db(db_cfg)
