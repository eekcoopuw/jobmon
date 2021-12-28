import getpass
import os
import sys

import pytest
import time
from unittest.mock import patch

from jobmon.client.tool import Tool
from jobmon.server.squid_integration.slurm_maxrss_queue import MaxrssQ
import jobmon.server.squid_integration.squid_integrator as squid
from jobmon.server.squid_integration.squid_integrator import IntegrationClusters
from jobmon.server.squid_integration.squid_utils import QueuedTI


def test_IntegrationClusters(db_cfg, ephemera):
    def fake_config():
        conn_str = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=ephemera["DB_USER"],
            pw=ephemera["DB_PASS"],
            host=ephemera["DB_HOST"],
            port=ephemera["DB_PORT"],
            db=ephemera["DB_NAME"],
        )
        return {"conn_str": conn_str, "polling_interval": 10, "max_update_per_sec": 10}

    with patch(
        "jobmon.server.squid_integration.squid_integrator._get_config", fake_config
    ):
        IntegrationClusters._cluster_type_instance_dict = {"dummy": None}
        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            sql = 'SELECT id FROM cluster_type WHERE name = "dummy"'
            ctid = DB.session.execute(sql).fetchone()["id"]
            i1 = IntegrationClusters.get_instance(DB.session, "dummy")
            assert i1.cluster_type_id == ctid
            sql = f"SELECT id FROM cluster WHERE cluster_type_id={ctid}"
            rows = DB.session.execute(sql)
            cids = [int(r["id"]) for r in rows]
            i2 = IntegrationClusters.get_instance(DB.session, "dummy")
            assert cids == i2.cluster_ids
            assert i1 is i2


@pytest.mark.skip(
    reason="This is not a regress test." "But useful to verify _get_squid_resource."
)
def test_get_slurm_resource_usages_on_slum():
    """This is to verify _get_squid_resource works.

    This test uses a hard coded job id.
    No need to run as regression.
    """
    r = os.system("scontrol")
    if r > 0:
        pytest.skip("This test only runs on slurm nodes.")
    else:
        import slurm_rest  # type: ignore
        from jobmon.server.squid_integration.squid_integrator import _get_squid_resource
        from jobmon.server.squid_integration.resilient_slurm_api import (
            ResilientSlurmApi as slurm,
        )

        # run a slurm job

        # a function to mock slurm auth token
        def get_slurm_api(*args):
            res = os.popen(f"scontrol token lifespan={300}").read()
            token = res.split("=")[1].strip()
            configuration = slurm_rest.Configuration(
                host="https://api-stage.cluster.ihme.washington.edu",
                api_key={
                    "X-SLURM-USER-NAME": getpass.getuser(),
                    "X-SLURM-USER-TOKEN": token,
                },
            )
            _slurm_api = slurm(slurm_rest.ApiClient(configuration))
            return _slurm_api

        with patch(
            "jobmon.server.squid_integration.squid_integrator._get_slurm_api",
            get_slurm_api,
        ):
            qti = QueuedTI()
            qti.distributor_id = 563173
            d = _get_squid_resource(qti)
            assert d["maxrss"] > 0
            assert d["wallclock"] > 0


@pytest.mark.skip(
    reason="This is not a regress test." "But useful to verify _get_squid_resource."
)
def test_get_slurm_resource_usages_via_api():
    import slurm_rest  # type: ignore
    from jobmon.server.squid_integration.squid_integrator import _get_squid_resource

    # run a slurm job
    qti = QueuedTI()
    qti.distributor_id = 13461
    d = _get_squid_resource(qti)
    assert d["maxrss"] > 0
    assert d["wallclock"] > 0


@pytest.mark.skip(reason="This case has issue in parallel running.")
def test_maxrss_forever(db_cfg, client_env, ephemera):
    from jobmon.server.squid_integration.squid_integrator import (
        _update_tis,
        _get_completed_task_instance,
    )

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
    workflow = tool.create_workflow(name="test_maxrss_forever")
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

    with patch(
        "jobmon.server.squid_integration.squid_integrator._get_config", fake_config
    ):
        IntegrationClusters._cluster_type_instance_dict = {"dummy": None}
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

            assert MaxrssQ.get_size() == 0

            # add completed tasks to Q
            _get_completed_task_instance(0, DB.session)
            assert MaxrssQ.get_size() == 5

            # update maxrss
            _update_tis(5, DB.session)
            assert MaxrssQ.get_size() == 0
            rows = DB.session.execute(sql).fetchall()
            assert rows is not None
            for r in rows:
                assert r["maxrss"] == "1314"
