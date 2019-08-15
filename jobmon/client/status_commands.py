import getpass
import pandas as pd

from jobmon.client import shared_requester


def workflow_status(workflow_id=[], user=[]):

    msg = {}
    if workflow_id:
        msg["workflow_id"] = workflow_id
    if user:
        msg["user"] = user
    else:
        msg["user"] = getpass.getuser()

    rc, res = shared_requester.send_request(
        app_route="/workflow_status",
        message=msg,
        request_type="get")
    return pd.read_json(res["workflows"])


def workflow_jobs(workflow_id, status=None):

    msg = {}
    if status:
        msg["status"] = status.upper()

    rc, res = shared_requester.send_request(
        app_route=f"/workflow/{workflow_id}/workflow_jobs",
        message=msg,
        request_type="get")
    return pd.read_json(res["workflow_jobs"])


def job_status(job_id):

    rc, res = shared_requester.send_request(
        app_route=f"/job/{job_id}/status",
        message={},
        request_type="get")
    return res["job_state"], pd.read_json(res["job_instance_status"])
