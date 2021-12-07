import pytest
from unittest.mock import patch

from jobmon.client.distributor.distributor_array import DistributorArray
from jobmon.client.distributor.distributor_workflow_run import _tiList, DistributorWorkflowRun
from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.constants import TaskInstanceStatus


def test_ds_arraysMap():
    """This is a unit test to test the data structure _arraysMap.

    Testing Data:
    ********************************************************************
    * tid   * Array id        * array_batch_id                          *
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
        * tid   * Array id        * array_batch_id     * distributor id     * subtask_id     *
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
                                  array_batch_id=1,
                                  distributor_id=1,
                                  subtask_id="1.1")
    ti2 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=2,
                                  array_id=2,
                                  array_batch_id=1,
                                  distributor_id=2,
                                  subtask_id="2.1")
    ti3 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=3,
                                  array_id=2,
                                  array_batch_id=1,
                                  distributor_id=2,
                                  subtask_id="2.2")
    ti4 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=4,
                                  array_id=2,
                                  array_batch_id=2,
                                  distributor_id=3,
                                  subtask_id="3.1")
    ti5 = DistributorTaskInstance(workflow_run_id=1,
                                  requester=None,
                                  cluster_type_id=1,
                                  task_instance_id=5,
                                  array_id=None,
                                  array_batch_id=None,
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
    dwfr = DistributorWorkflowRun(workflow_id=1, workflow_run_id=1, requester=None)
    ti1 = DistributorTaskInstance(task_instance_id=1, workflow_run_id=1, requester=None,
                                  distributor_id=1, cluster_type_id=1)
    ti2 = DistributorTaskInstance(task_instance_id=2, workflow_run_id=1, requester=None,
                                  distributor_id=2, cluster_type_id=1)
    ti3 = DistributorTaskInstance(task_instance_id=3, workflow_run_id=1, requester=None,
                                  distributor_id=3, cluster_type_id=1)
    ti101 = DistributorTaskInstance(task_instance_id=101, workflow_run_id=1, requester=None,
                                    distributor_id=10, array_id=1, cluster_type_id=1)
    ti102 = DistributorTaskInstance(task_instance_id=102, workflow_run_id=1, requester=None,
                                    distributor_id=10, array_id=1, cluster_type_id=1)
    ti201 = DistributorTaskInstance(task_instance_id=201, workflow_run_id=1, requester=None,
                                    distributor_id=20, array_id=2, cluster_type_id=1)
    ti202 = DistributorTaskInstance(task_instance_id=202, workflow_run_id=1, requester=None,
                                    distributor_id=20, array_id=2, cluster_type_id=1)
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
    ti_list.extend([1, 2, 3])
    dwfr._launched_task_instance_ids = ti_list
    ti_list_array = _tiList()
    ti_list_array.extend([101, 102, 201, 202])
    dwfr._launched_array_task_instance_ids = ti_list_array

    # mimic a heart beat function for testing
    def _heartbeat():
        # check launching queue
        # sync with DB
        ti_dict = dwfr.refresh_status_from_db(dwfr._launched_task_instance_ids, "B", False)
        for tiid in ti_dict.keys():
            dwfr._launched_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "R":
                # move to running Q
                dwfr._running_task_instance_ids.add(tiid)
            elif ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] == "I":
                raise Exception("No way this should happen.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = dwfr.refresh_status_with_distributor(dwfr._launched_task_instance_ids, "B", False)
        for tiid in ti_dict.keys():
            if ti_dict[tiid] == "R":
                # do nothing
                pass
            else:
                dwfr._launched_task_instance_ids.pop(tiid)
                if ti_dict[tiid] == "D":
                    pass
                elif ti_dict[tiid] == "I":
                    raise Exception("No way this should happen.")
                else:
                    dwfr.wfr_has_failed_tis = True
                    dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        dwfr.transition_task_instance(ti_dict)

        # check running queue
        # sync with DB
        ti_dict = dwfr.refresh_status_from_db(dwfr._running_task_instance_ids, "R", False)
        for tiid in ti_dict.keys():
            dwfr._running_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] in ("I", "B"):
                raise Exception("No way this should happen.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = dwfr.refresh_status_with_distributor(dwfr._running_task_instance_ids, "R", False)
        for tiid in ti_dict.keys():
            dwfr._running_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] in ("B"):
                raise Exception("The cluster much be crazy.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        dwfr.transition_task_instance(ti_dict)

        # check launching queue array
        # sync with DB
        ti_dict = dwfr.refresh_status_from_db(dwfr._launched_array_task_instance_ids, "B", True)
        for tiid in ti_dict.keys():
            dwfr._launched_array_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "R":
                # move to running Q
                dwfr._running_array_task_instance_ids.add(tiid)
            elif ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] == "I":
                raise Exception("No way this should happen.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = dwfr.refresh_status_with_distributor(dwfr._launched_array_task_instance_ids, "B", True)
        for tiid in ti_dict.keys():
            if ti_dict[tiid] == "R":
                # do nothing
                pass
            else:
                dwfr._launched_array_task_instance_ids.pop(tiid)
                if ti_dict[tiid] == "D":
                    pass
                elif ti_dict[tiid] == "I":
                    raise Exception("No way this should happen.")
                else:
                    dwfr.wfr_has_failed_tis = True
                    dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        dwfr.transition_task_instance(ti_dict)

        # check running queue array
        # sync with DB
        ti_dict = dwfr.refresh_status_from_db(dwfr._running_array_task_instance_ids, "R", True)
        for tiid in ti_dict.keys():
            dwfr._running_array_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] in ("I", "B"):
                raise Exception("No way this should happen.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        # sync with distributor
        # only check those unchanged in DB
        ti_dict = dwfr.refresh_status_with_distributor(dwfr._running_array_task_instance_ids, "R", True)
        for tiid in ti_dict.keys():
            dwfr._running_array_task_instance_ids.pop(tiid)
            if ti_dict[tiid] == "D":
                pass
            elif ti_dict[tiid] in ("B"):
                raise Exception("The cluster much be crazy.")
            else:
                dwfr.wfr_has_failed_tis = True
                dwfr._map.get_DistributorTaskInstance_by_id(tiid).error_state = ti_dict[tiid]
        dwfr.transition_task_instance(ti_dict)

    # mock functions
    def mock_transition_ti(*args):
        pass

    with patch.object(dwfr,
                      'transition_task_instance',
                      side_effect=mock_transition_ti):
        # FIRST HEAT BEAT
        def mock_refresh_db(*args):
            status = args[1]
            is_array = args[2]
            if is_array:
                if status == "B":
                    return {101: "R", 201: "D"}
                else:
                    return {}
            else:
                if status == "B":
                    return {1: "R", 2: "R", 3: "R"}
                else:
                    return {}

        def mock_refresh_distributor(*args):
            status = args[1]
            is_array = args[2]
            if is_array:
                if status == "B":
                    return {102: "R"}
                else:
                    return {}
            else:
                if status == "B":
                    return {}
                else:
                    return {}
        with patch.object(dwfr,
                          'refresh_status_from_db',
                          side_effect=mock_refresh_db):
            with patch.object(dwfr,
                              'refresh_status_with_distributor',
                              side_effect=mock_refresh_distributor):
                              _hearrtrbeat()
                              # {1: "R", 2: "R", 3: "R", 101: "R", 102: "B", 201: "D", 202: "B"}
                              assert dwfr._launched_task_instance_ids.length == 0
                              assert dwfr._running_task_instance_ids.length == 3
                              assert set(dwfr._launched_array_task_instance_ids.ids) == {102, 202}
                              assert set(dwfr._running_array_task_instance_ids.ids) == {101}
                              assert not dwfr.wfr_has_failed_tis

        # SECOND HEAT BEAT
        def mock_refresh_db(*args):
            status = args[1]
            is_array = args[2]
            if is_array:
                if status == "B":
                    return {102: "R"}
                elif status == "R":
                    return {101: "D"}
                else:
                    return {}
            else:
                if status == "B":
                    return {}
                elif status == "R":
                    return {2: "D"}
                else:
                    return {}

        def mock_refresh_distributor(*args):
            status = args[1]
            is_array = args[2]
            if is_array:
                if status == "B":
                    return {202: "U"}
                else:
                    return {}
            else:
                if status == "R":
                    return {3: "D"}
                else:
                    return {}
        with patch.object(dwfr,
                          'refresh_status_from_db',
                          side_effect=mock_refresh_db):
            with patch.object(dwfr,
                              'refresh_status_with_distributor',
                              side_effect=mock_refresh_distributor):
                              _heartrbeat()
                              # {1: "R", 2: "D", 3: "D", 101: "D", 102: "R", 201: "D", 202: "U"}
                              assert dwfr._launched_task_instance_ids.length == 0
                              assert set(dwfr._running_task_instance_ids.ids) == {1}
                              assert dwfr._launched_array_task_instance_ids.length == 0
                              assert set(dwfr._running_array_task_instance_ids.ids) == {102}
                              assert dwfr.wfr_has_failed_tis

