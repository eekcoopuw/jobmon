from jobmon import BashTask
from jobmon import Workflow

from jobmon.client import shared_requester as req
from jobmon.server.job_visualization_server.job_visualization_server import (
    _viz_label_mapping)


def test_job_status(real_jsm_jqs, db_cfg):
    rc, resp = req.send_request(
        app_route='/job_status',
        message={},
        request_type='get')
    assert len(resp["job_statuses_dict"]) > 0
    for job_status in resp["job_statuses_dict"]:
        assert job_status["label"] in _viz_label_mapping.values()


def test_foo(real_jsm_jqs, db_cfg):
    t1 = BashTask("sleep 10", num_cores=1)
    t2 = BashTask("sleep 5", upstream_tasks=[t1], num_cores=1)
    workflow = Workflow()
    workflow.add_tasks([t1, t2])
    workflow._bind()

    # we should have the column headers plus 2 tasks
    rc, resp = req.send_request(
        app_route=f'/workflow/{workflow.id}/job_display_details',
        message={},
        request_type='get')
    assert len(resp["jobs"]) == 3
    last_sync = resp["time"]

    workflow.run()

    # now each of our jobs should be in D state
    rc, resp = req.send_request(
        app_route=f'/workflow/{workflow.id}/job_display_details',
        message={"last_sync": last_sync},
        request_type='get')
    jobs = resp["jobs"]

    # zero index in responses is column names so ignore
    for job in jobs[1:]:
        # first index is job status which should have moved to done
        assert job[1] == "D"
