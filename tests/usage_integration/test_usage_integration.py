import datetime
import time

import pytest
from unittest import mock

from jobmon.client.tool import Tool
from jobmon.server.usage_integration.usage_integrator import _get_slurm_resource_via_slurm_sdb
from jobmon.server.usage_integration.usage_queue import UsageQ
from jobmon.server.usage_integration.usage_utils import QueuedTI
from jobmon.server.usage_integration.usage_integrator import UsageIntegrator as UI


@pytest.mark.usage_integrator
@pytest.mark.skip("This test no longer apply after switching to slurm db")
def test_get_slurm_resource_usages_on_slurm(usage_integrator):
    """This is to verify _get_squid_resource works.

    This test uses a hard coded job id.
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
def test_queue_map_cache(usage_integrator_config):
    with mock.patch(
        "jobmon.server.usage_integration.usage_integrator._get_slurm_resource_via_slurm_sdb",
    ) as m_get_resc, mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator._get_tres_types"
    )as m_tres_type, mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator.update_resources_in_db"
    ) as m_db, mock.patch(
        "jobmon.server.usage_integration.usage_integrator.UsageIntegrator.populate_queue"
    ) as m_restful:
        # mock
        m_get_resc.return_value = {}
        m_db.return_value = None
        m_restful.return_value = None
        m_tres_type.return_value = None

        usage_integrator = UI(usage_integrator_config)
        queue_cache = usage_integrator.queue_cluster_map
        assert usage_integrator._queue_cluster_map is not None
        assert id(queue_cache) == id(usage_integrator.queue_cluster_map)  # Check cache was hit
        assert len(queue_cache) == 8  # 8 queues defined in the Jobmon test_utils schema

        # Cherrypick a few test values
        assert queue_cache[1] == (2, 'sequential')  # null.q sequential cluster
        assert queue_cache[3] == (1, 'dummy')  # null.q dummy cluster
        assert queue_cache[8] == (3, 'multiprocess')  # null.q multiprocess cluster


@pytest.mark.usage_integrator
def test_maxrss_forever(db_cfg, client_env, ephemera, usage_integrator_config):
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

        with mock.patch(
            "jobmon.server.usage_integration.usage_integrator.UsageIntegrator._get_tres_types"
        ) as m_tres_type:
            # mock
            m_tres_type.return_value = None

            usage_integrator = UI(usage_integrator_config)
            assert UsageQ.get_size() == 0

            # add completed tasks to Q
            usage_integrator.populate_queue(datetime.datetime.fromtimestamp(0))
            assert UsageQ.get_size() == 5

            # update maxrss
            task_instances = [UsageQ.get() for _ in range(5)]
            assert UsageQ.get_size() == 0
            usage_integrator.update_resources_in_db(task_instances)
            rows = DB.session.execute(sql).fetchall()
            assert rows is not None
            for r in rows:
                assert r["maxrss"] == "1314"

            # Check that populate queue can filter on time as expected
            insert_sql = (
                "INSERT INTO task_instance(task_id, status, distributor_id, task_resources_id, status_date) "
                f"VALUES ({t.task_id}, 'D', 123456, {t._original_task_resources.id}, NOW())"
            )
            usage_integrator.session.execute(insert_sql)
            usage_integrator.session.commit()

            # Sleep 1 seconds, record the current time, sleep another second.
            # Done since timestamps are most granular by second, so each distinct row needs at
            # least a 1 second offset in order to be meaningful.

            # Ex. without sleeps, we will likely generate 2 rows in the DB and the current time
            # within 1 second. Current_time = ti1.status_date = ti2.status_date
            # We want to enforce that ti1.status_date < current_time < ti2.status_date
            time.sleep(1)
            current_time = datetime.datetime.today()
            time.sleep(1)

            # Do it again to generate a second task instance
            usage_integrator.session.execute(insert_sql)
            usage_integrator.session.commit()

            # We should have 2 eligible task instances in the database, one before current_time
            # and one after. Call populate queue to check that one and only one is picked up
            UsageQ.empty_q()
            usage_integrator.populate_queue(current_time)
            assert UsageQ.get_size() == 1
