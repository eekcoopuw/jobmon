from multiprocessing import Queue
from unittest.mock import patch

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_workflow_run import _tiList, DistributorWorkflowRun
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.exceptions import ResumeSet

import pytest

from jobmon.client.tool import Tool


@pytest.fixture
def tool(db_cfg, client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="my_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    return tt


class MockDistributorProc:
    def is_alive(self):
        return True


def test_ds_arraysMap():
    """This is a unit test to test the data structure _arraysMap.

    Testing Data:
    ********************************************************************
    * tid   * Array id        * array_batch_num                          *
    * 1     * 1               * 1                                   *
    * 2     * 2               * 1                                      *
    * 3     * 2               * 1                                      *
    * 4     * 2               * 2                                      *
    ********************************************************************
    _multiwayMap: {1:{1: [1]}, 2: {1: [2, 3], 2: [4]}}
    """
    from jobmon.client.distributor.distributor_workflow_run import _arraysMap
    m = _arraysMap()
    m.add_new_element(1, 1, 1)
    m.add_new_element(2, 1, 2)
    m.add_new_element(2, 1, 3)
    m.add_new_element(2, 2, 4)
    assert m._all_data == {1: {1: [1]}, 2: {1: [2, 3], 2: [4]}}
    assert m._all_value == [1, 2, 3, 4]
    assert m.get_tis_by_array(1) == [1]
    assert m.get_tis_by_array(2) == [2, 3, 4]
    assert m.get_tis_by_array_batch(2, 2) == [4]


def test_WorkflowRunMaps():
    """This is a unit test to test the data structure WorkflowRunMaps.

        Testing Data:
        **************************************************************************************
        * tid   * Array id        * array_batch_num     * distributor id     * subtask_id     *
        * 1     * 1               * 1                  * 1                  * 1.1              *
        * 2     * 2               * 1                  * 2                  * 2.1            *
        * 3     * 2               * 1                  * 2                  * 2.2            *
        * 4     * 2               * 2                  * 3                  * 3.1            *
        * 5     * None            * None               * 4                  * 4
        ********************************************************************
    """
    dwfr = DistributorWorkflowRun(workflow_id=1, workflow_run_id=1, requester=None)
    ti1 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=1,
                                  array_id=1,
                                  array_batch_num=1,
                                  distributor_id=1,
                                  subtask_id="1.1")
    ti2 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=2,
                                  array_id=2,
                                  array_batch_num=1,
                                  distributor_id=2,
                                  subtask_id="2.1")
    ti3 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=3,
                                  array_id=2,
                                  array_batch_num=1,
                                  distributor_id=2,
                                  subtask_id="2.2")
    ti4 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=4,
                                  array_id=2,
                                  array_batch_num=2,
                                  distributor_id=3,
                                  subtask_id="3.1")
    ti5 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=5,
                                  array_id=None,
                                  array_batch_num=None,
                                  distributor_id=4,
                                  subtask_id="4")
    a1 = DistributorArray(array_id=1, task_resources_id=1, requested_resources=[], requester=None)
    a2 = DistributorArray(array_id=2, task_resources_id=2, requested_resources=[], requester=None)
    dwfr._map.add_DistributorTaskInstance(ti1)
    dwfr._map.add_DistributorTaskInstance(ti2)
    dwfr._map.add_DistributorTaskInstance(ti3)
    dwfr._map.add_DistributorTaskInstance(ti4)
    dwfr._map.add_DistributorTaskInstance(ti5)
    dwfr._map.add_DistributorArray(a1)
    dwfr._map.add_DistributorArray(a2)
    assert len(dwfr._map.get_task_instances()) == 5
    assert set(dwfr._map.get_task_instance_ids()) ==  {1, 2, 3, 4, 5}
    assert dwfr._map.get_DistributorTaskInstance_by_id(1) == ti1
    assert dwfr._map.get_DistributorTaskInstance_by_subtaskid("3.1") == ti4
    assert dwfr._map.get_array_batch_DistributorTaskInstance(2, 2)[0] == ti4
    assert dwfr._map.get_array_DistributorTaskInstance(1)[0] == ti1
    assert len(dwfr._map.get_array_DistributorTaskInstance(2)) == 3
    assert len(dwfr._map.get_DistributorTaskInstance_by_did(2)) == 2
    assert dwfr._map.get_DistributorTaskInstance_by_did(4)[0] == ti5
    assert dwfr._map.get_DistributorArray_by_tiid(1) == a1


def test_wfr_heartbeat_flow():
    """This is to drive the development.

    If uses mocked method to mimic the wfr heatbeat process.
    It only meaningful when the steps matches the logic in wfr heatbeat.
    """

    # Prepare fake distributor data
    """
    Testing  Data:
    ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** 
    *tid  * Array id * array_batch_num * array_step_id * distributor id * subtask_id   *
    * 1   * None     * None           * None          * 1              * 1            *
    * 2   * None     * None           * None          * 2              * 2            *
    * 3   * None     * None           * None          * 3              * 3            *
    * 101 * 1        * 1              * 1             * 10             * 10.1         *
    * 102 * 1        * 1              * 2             * 10             * 10.2         *
    * 201 * 2        * 1              * 1             * 20             * 20.1         *
    * 202 * 2        * 1              * 2             * 20             * 20.2         *
    ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** 
    """
    dwfr = DistributorWorkflowRun(workflow_id=1, workflow_run_id=1, requester=None)
    ti1 = DistributorTaskInstance(task_instance_id=1, workflow_run_id=1, requester=None,
                                  distributor_id=1, cluster_type_id=1, subtask_id="1")
    ti2 = DistributorTaskInstance(task_instance_id=2, workflow_run_id=1, requester=None,
                                  distributor_id=2, cluster_type_id=1, subtask_id="2")
    ti3 = DistributorTaskInstance(task_instance_id=3, workflow_run_id=1, requester=None,
                                  distributor_id=3, cluster_type_id=1, subtask_id="3")
    ti101 = DistributorTaskInstance(task_instance_id=101, workflow_run_id=1, requester=None,
                                    distributor_id=10, array_id=1, cluster_type_id=1,
                                    array_batch_num=1, array_step_id=1, subtask_id="10.1")
    ti102 = DistributorTaskInstance(task_instance_id=102, workflow_run_id=1, requester=None,
                                    distributor_id=10, array_id=1, cluster_type_id=1,
                                    array_batch_num=1, array_step_id=2, subtask_id="10.2")
    ti201 = DistributorTaskInstance(task_instance_id=201, workflow_run_id=1, requester=None,
                                    distributor_id=20, array_id=2, cluster_type_id=1,
                                    array_batch_num=1, array_step_id=1, subtask_id="20.1")
    ti202 = DistributorTaskInstance(task_instance_id=202, workflow_run_id=1, requester=None,
                                    distributor_id=20, array_id=2, cluster_type_id=1,
                                    array_batch_num=1, array_step_id=2, subtask_id="20.2")
    a1 = DistributorArray(array_id=1, task_resources_id=1, requested_resources=[], requester=None)
    a2 = DistributorArray(array_id=2, task_resources_id=2, requested_resources=[], requester=None)
    dwfr._map.add_DistributorTaskInstance(ti1)
    dwfr._map.add_DistributorTaskInstance(ti2)
    dwfr._map.add_DistributorTaskInstance(ti3)
    dwfr._map.add_DistributorTaskInstance(ti101)
    dwfr._map.add_DistributorTaskInstance(ti102)
    dwfr._map.add_DistributorTaskInstance(ti201)
    dwfr._map.add_DistributorTaskInstance(ti202)
    dwfr._map.add_DistributorArray(a1)
    dwfr._map.add_DistributorArray(a2)
    # assume all tis are launched before starting
    ti_list =_tiList()
    ti_list.extend([1, 2, 3, 101, 102, 201, 202])
    dwfr._launched_task_instance_ids = ti_list

    # mock functions
    def mock_transition_ti(*args):
        pass

    with patch.object(dwfr,
                      'transition_task_instance',
                      side_effect=mock_transition_ti):
        # FIRST HEAT BEAT
        def mock_refresh_db(*args):
            status = args[1]
            if status == "B":
                return {1: "R", 2: "R", 3: "R", 101: "R", 201: "D"}
            else:
                return {}

        def mock_refresh_distributor(*args):
            status = args[1]
            if status == "B":
                return {102: "R"}
            else:
                return {}

        with patch.object(dwfr,
                          'refresh_status_from_db',
                          side_effect=mock_refresh_db):
            with patch.object(dwfr,
                              'refresh_status_with_distributor',
                              side_effect=mock_refresh_distributor):
                              dwfr.heartbeat()
                              # {1: "R", 2: "R", 3: "R", 101: "R", 102: "B", 201: "D", 202: "B"}
                              assert set(dwfr._launched_task_instance_ids.tis) == {102, 202}
                              assert set(dwfr._running_task_instance_ids.tis) == {1, 2, 3, 101}
                              assert not dwfr.wfr_has_failed_tis

        # SECOND HEAT BEAT
        def mock_refresh_db(*args):
            status = args[1]
            if status == "B":
                return {102: "R"}
            elif status == "R":
                return {2: "D", 101: "D"}
            else:
                return {}

        def mock_refresh_distributor(*args):
            status = args[1]
            if status == "B":
                return {202: "U"}
            else:
                return {3: "D"}

        with patch.object(dwfr,
                          'refresh_status_from_db',
                          side_effect=mock_refresh_db):
            with patch.object(dwfr,
                              'refresh_status_with_distributor',
                              side_effect=mock_refresh_distributor):
                              dwfr.heartbeat()
                              # {1: "R", 2: "D", 3: "D", 101: "D", 102: "R", 201: "D", 202: "U"}
                              assert set(dwfr._launched_task_instance_ids.tis) == set()
                              assert set(dwfr._running_task_instance_ids.tis) == {1, 102}
                              assert set(dwfr._error_task_instance_ids.tis) == {202}
                              assert dwfr.wfr_has_failed_tis



def test_heartbeat(tool, db_cfg, client_env, task_template):
    """test that the TaskInstanceDistributor logs a heartbeat in the database"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="my_beating_heart")
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )
    distributor_service.heartbeat()

    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        SELECT workflow_run.heartbeat_date > CURRENT_TIMESTAMP()
        FROM workflow_run
        WHERE workflow_run.id = :workflow_run_id"""
        res = DB.session.execute(
            sql, {"workflow_run_id": wfr.workflow_run_id}
        ).fetchone()
        DB.session.commit()
    assert res[0] == 1


def test_heartbeat_raises_error(tool, db_cfg, client_env, task_template):
    """test that a heartbeat logged after resume will raise ResumeSet"""
    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="my_heartbeat_error")
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )
    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE workflow_run
        SET status = 'C'
        WHERE workflow_run.id = :workflow_run_id"""
        DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id})
        DB.session.commit()

    with pytest.raises(ResumeSet):
        distributor_service.heartbeat()


def test_heartbeat_propagate_error(tool, db_cfg, client_env, task_template):
    """test that a heartbeat logged after resume will raise ResumeSet through
    the message queue and can be re_raised"""

    from jobmon.client.distributor.distributor_service import DistributorService
    from jobmon.cluster_type.sequential.seq_distributor import SequentialDistributor
    from jobmon.requester import Requester

    t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
    workflow = tool.create_workflow(name="heartbeat_propagate_error")
    workflow.add_tasks([t1])
    workflow.bind()
    wfr = workflow._create_workflow_run()

    requester = Requester(client_env)
    distributor_service = DistributorService(
        workflow.workflow_id,
        wfr.workflow_run_id,
        SequentialDistributor(),
        requester=requester,
    )
    # check the job finished
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
        UPDATE workflow_run
        SET status = 'C'
        WHERE workflow_run.id = :workflow_run_id"""
        DB.session.execute(sql, {"workflow_run_id": wfr.workflow_run_id})
        DB.session.commit()

    q = Queue()
    distributor_service.run_distributor(status_queue=q)
    assert q.get() == "ALIVE"

    with pytest.raises(ResumeSet):
        q.get().re_raise()
