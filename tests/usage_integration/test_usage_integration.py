import getpass
import os
import time

import pytest
from unittest.mock import patch

from jobmon.client.tool import Tool
from jobmon.server.usage_integration.usage_integrator import (
    UsageIntegrator, _get_slurm_resource_via_slurm_sdb)
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI


@pytest.mark.usage_integrator
def test_get_slurm_resource_usages_on_slurm(usage_integrator):
    """This is to verify _get_squid_resource works.

    This test uses a hard coded job id.
    No need to run as regression.
    """

    # Random non array distributor id
    distributor_id = '7931516'
    qti = QueuedTI(
        task_instance_id=1,
        distributor_id=distributor_id,
        cluster_type_name="slurm",
        cluster_id=1,
    )
    d = _get_slurm_resource_via_slurm_sdb(
        session=usage_integrator.session_slurm_sdb,
        tres_types=usage_integrator.tres_types,
        task_instances=[qti]
    )
    # Known values
    assert d[qti]["maxrss"] == 3721453568
    assert d[qti]["wallclock"] == 214

    # Query with a non array task as well
    qti_array = QueuedTI(
        task_instance_id=2,
        distributor_id='7869919_4',
        cluster_type_name='slurm',
        cluster_id=1,
    )

    array_nonarray_values = _get_slurm_resource_via_slurm_sdb(
        session=usage_integrator.session_slurm_sdb,
        tres_types=usage_integrator.tres_types,
        task_instances=[qti, qti_array]
    )
    assert array_nonarray_values[qti] == d[qti]
    assert array_nonarray_values[qti_array]["maxrss"] == 0
    assert array_nonarray_values[qti_array]["wallclock"] == 7


@pytest.mark.usage_integrator
def test_maxrss_forever(db_cfg, client_env, ephemera, usage_integrator):
    """Note: Do not run usage_integrator tests with multiprocessing."""
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

    # CLear the usage q
    UsageQ.empty_q()

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

        # add completed tasks to Q
        usage_integrator.populate_queue(0)
        assert UsageQ.get_size() == 5

        # update maxrss
        task_instances = [UsageQ.get() for _ in range(5)]
        assert UsageQ.get_size() == 0
        usage_integrator.update_resources_in_db(task_instances)
        rows = DB.session.execute(sql).fetchall()
        assert rows is not None
        for r in rows:
            assert r["maxrss"] == "1314"
