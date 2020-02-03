import getpass
import pandas as pd
from typing import List, Tuple

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging


logger = logging.getLogger(__name__)


def workflow_status(workflow_id: List[int] = [], user: List[str] = []
                    ) -> pd.DataFrame:
    """Get metadata about workflow progress

    Args:
        workflow_id:
        user:

    Returns:
        dataframe of all workflows and their status
    """
    logger.debug("workflow_status workflow_id:{}".format(str(workflow_id)))
    msg: dict = {}
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


def workflow_jobs(workflow_id: int, status: str = None) -> pd.DataFrame:
    """Get metadata about job state for a given workflow

    Args:
        workflow_id:
        user:

    Returns:
        Dataframe of jobs for a given workflow
    """
    logger.info("workflow id: {}".format(workflow_id))
    msg = {}
    if status:
        msg["status"] = status.upper()

    rc, res = shared_requester.send_request(
        app_route=f"/workflow/{workflow_id}/workflow_jobs",
        message=msg,
        request_type="get")
    return pd.read_json(res["workflow_jobs"])


def job_status(job_id: int) -> Tuple[str, pd.DataFrame]:
    """Get metadata about a job and its job isntances

    Args:
        job_id: a job_id to retrieve job_instance metadata for

    Returns:
        Job status and job_instance metadata
    """
    logger.info("job_status job_id:{}".format(job_id))
    rc, res = shared_requester.send_request(
        app_route=f"/job/{job_id}/status",
        message={},
        request_type="get")
    return res["job_state"], pd.read_json(res["job_instance_status"])
