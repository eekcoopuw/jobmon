import getpass
import os
import time

import pytest
from unittest.mock import patch

from jobmon.client.tool import Tool
from jobmon.server.usage_integration.usage_integrator import UsageIntegrator
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI


@pytest.mark.skip(
    reason="This is not a regress test." "But useful to verify _get_squid_resource."
)
def test_get_slurm_resource_usages_on_slurm():
    """This is to verify _get_squid_resource works.

    This test uses a hard coded job id.
    No need to run as regression.
    """
    r = os.system("scontrol")
    if r > 0:
        pytest.skip("This test only runs on slurm nodes.")
    else:
        import slurm_rest  # type: ignore
        from jobmon.server.usage_integration.usage_integrator import _get_squid_resource
        from jobmon.server.usage_integration.resilient_slurm_api import (
            ResilientSlurmApi as slurm,
        )

        # a function to mock slurm auth token
        def get_slurm_api(*args):
            res = os.popen(f"scontrol token lifespan={300}").read()
            token = res.split("=")[1].strip()
            configuration = slurm_rest.Configuration(
                host="https://api.cluster.ihme.washington.edu",
                api_key={
                    "X-SLURM-USER-NAME": getpass.getuser(),
                    "X-SLURM-USER-TOKEN": token,
                },
            )
            _slurm_api = slurm(slurm_rest.ApiClient(configuration))
            return _slurm_api

        slurm_api = get_slurm_api()
        d = _get_squid_resource(slurm_api=slurm_api, distributor_id=563173)
        assert d["maxrss"] > 0
        assert d["wallclock"] > 0


# @pytest.mark.skip(reason="Probalem to run in parallel.")
def test_maxrss_forever(db_cfg, client_env, ephemera):

    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="dummy", compute_resources={"queue": "null.q"}
    )
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    workflow = tool.create_workflow(name="test_q_forever")
    tasks = []
    for i in range(5):
        t = tt.create_task(arg=f"echo {i}")
        tasks.append(t)
    workflow.add_tasks(tasks)
    workflow_run_status = workflow.run()
    assert workflow_run_status == "D"

    # time to mock
    def fake_config():
        conn_str = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=ephemera["DB_USER"],
            pw=ephemera["DB_PASS"],
            host=ephemera["DB_HOST"],
            port=ephemera["DB_PORT"],
            db=ephemera["DB_NAME"],
        )
        return {"conn_str": conn_str, "polling_interval": 1, "max_update_per_sec": 10}

    # CLear the usage q
    UsageQ.empty_q()

    with patch(
        "jobmon.server.usage_integration.usage_integrator._get_config", fake_config
    ):
        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            tis = []
            tids = [t.task_id for t in tasks]
            sql = f"""
                SELECT id, maxrss
                FROM task_instance
                WHERE task_id in {str(tids).replace("[", "(").replace("]", ")")}
            """
            rows = DB.session.execute(sql).fetchall()
            assert rows is not None
            for r in rows:
                assert r["maxrss"] is None
                tis.append(int(r["id"]))
            # set a -1
            sql_update = f"""
                UPDATE task_instance
                SET maxrss=-1
                WHERE id={tis[0]}
            """
            DB.session.execute(sql_update)
            DB.session.commit()
            # set a 0
            sql_update = f"""
                UPDATE task_instance
                SET maxrss=0
                WHERE id={tis[1]}
                """
            DB.session.execute(sql_update)
            DB.session.commit()

            assert UsageQ.get_size() == 0

            try:
                integrator = UsageIntegrator()
                # add completed tasks to Q
                integrator.populate_queue(0)
                assert UsageQ.get_size() == 5

                # update maxrss
                task_instances = [UsageQ.get() for _ in range(5)]
                assert UsageQ.get_size() == 0
                integrator.update_resources_in_db(task_instances)
                rows = DB.session.execute(sql).fetchall()
                assert rows is not None
                for r in rows:
                    assert r["maxrss"] == "1314"
            finally:
                integrator.session.close()


@pytest.mark.skip()
def test_usage_integrator(db_cfg, ephemera):

    from jobmon.server.usage_integration.usage_queue import UsageQ

    def fake_config():
        conn_str = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=ephemera["DB_USER"],
            pw=ephemera["DB_PASS"],
            host=ephemera["DB_HOST"],
            port=ephemera["DB_PORT"],
            db=ephemera["DB_NAME"],
        )
        return {"conn_str": conn_str, "polling_interval": 1, "max_update_per_sec": 10}

    with patch(
        "jobmon.server.usage_integration.usage_integrator._get_config", fake_config
    ):
        integrator = UsageIntegrator()
        integrator._connection_params = {"slurm_rest_host": "https://api.cluster.ihme.washington.edu",
                                         "slurmtool_token_host": "https://slurmtool.ihme.washington.edu/api/v1/token/"}
        api_1 = integrator.slurm_api # Should generate the object
        api_2 = integrator.slurm_api # Should pull from the cache

        assert id(api_1) == id(api_2)


        # Test a populate queue call
        # Insert some dummy task instances
        task_instances = [
            # (task_instance_id, task_id, status, distributor_id, cluster_type_id)
            (1, 1, 'D', 1, 1),
            (2, 1, 'D', 2, 1),
            (3, 1, 'D', 3, 1),
            (4, 1, 'D', 4, 1)
        ]

        add_task_instances = """
            INSERT INTO task_instance(id, task_id, status, distributor_id, cluster_type_id)
            VALUES {}
        """

        app, DB = db_cfg['app'], db_cfg['DB']
        with app.app_context():
            DB.session.execute(add_task_instances.format(','.join([str(t) for t in task_instances])))

            # Update the timestamps to mark them ready for completion
            update_stmt = """
                UPDATE task_instance
                SET status_date = CURRENT_TIMESTAMP()
                WHERE id IN (1,2,3,4)
            """
            DB.session.execute(update_stmt)
            DB.session.commit()

    # Clear the usageq in case there are leftover artifacts
    UsageQ.empty_q()

    # Test populating the queue
    integrator.populate_queue(time.time() - 1000)
    assert UsageQ.get_size() == 4