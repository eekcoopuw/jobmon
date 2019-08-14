from jobmon import BashTask
from jobmon import Workflow
from jobmon.cli import CLI
from jobmon.client import shared_requester as req


def test_foo(real_jsm_jqs, db_cfg):
    t1 = BashTask("sleep 10", num_cores=1)
    t2 = BashTask("sleep 5", upstream_tasks=[t1], num_cores=1)
    workflow = Workflow()
    workflow.add_tasks([t1, t2])
    workflow._bind()

    # we should have the column headers plus 2 tasks
    command_str = "jobmon workflow_status -u mlsandar -w 1"
    cli = CLI()
    args = cli.parse_args(command_str)
    args.func(args)

    workflow.run()
    assert 0

    # # now each of our jobs should be in D state
    # rc, resp = req.send_request(
    #     app_route=f'/workflow/{workflow.id}/job_display_details',
    #     message={"last_sync": last_sync},
    #     request_type='get')
    # jobs = resp["jobs"]

    # # zero index in responses is column names so ignore
    # for job in jobs[1:]:
    #     # first index is job status which should have moved to done
    #     assert job[1] == "DONE"
