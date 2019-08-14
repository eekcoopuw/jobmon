from collections.abc import Iterable
from http import HTTPStatus as StatusCodes
from flask import jsonify, request, Blueprint
import logging
import pandas as pd


from jobmon.models import DB
from jobmon.models.workflow import Workflow
from jobmon.models.job_status import JobStatus


jvs = Blueprint("job_visualization_server", __name__)


logger = logging.getLogger(__name__)


def get_time(session):
    time = session.execute("select UTC_TIMESTAMP as time").fetchone()['time']
    time = time.strftime("%Y-%m-%d %H:%M:%S")
    return time


_viz_label_mapping = {
    "A": "PENDING",
    "G": "PENDING",
    "Q": "PENDING",
    "I": "PENDING",
    "E": "PENDING",
    "R": "RUNNING",
    "F": "FATAL",
    "D": "DONE"
}
_viz_order = ["PENDING", "RUNNING", "DONE", "FATAL"]


@jvs.route('/job_status', methods=['GET'])
def get_job_statuses():
    """get job status metadata"""
    job_statuses = DB.session.query(JobStatus).all()
    DB.session.commit()

    # remap to viz names
    job_status_set = set()
    job_status_wire = []
    for job_status_db in job_statuses:
        label = _viz_label_mapping[job_status_db.id]
        if label not in job_status_set:
            job_status_set.add(label)
            job_status = {}
            job_status["label"] = label
            job_status["order"] = _viz_order.index(label)
            job_status_wire.append(job_status)

    # send to client
    resp = jsonify(job_statuses_dict=job_status_wire)
    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow', methods=['GET'])
def get_workflows_by_status():
    """get all workflows with a given status

    Args:
        status (list, None): list of valid statuses from WorkflowStatus. If
            None, then all workflows are returned
    """
    if request.args.get('status', None) is not None:
        workflows = DB.session.query(Workflow)\
            .filter(Workflow.status.in_(request.args.getlist('status')))\
            .all()
    else:
        workflows = DB.session.query(Workflow).all()

    workflow_dcts = [w.to_wire() for w in workflows]
    logger.info("workflow_dcts={}".format(workflow_dcts))
    resp = jsonify(workflow_dcts=workflow_dcts)
    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow/<workflow_id>/job_display_details', methods=['GET'])
def get_job_display_details_by_workflow(workflow_id):
    """Get the jobs that have changed status since a given time for a workflow.

    Args:
        last_sync (datetime, None): get all jobs that have been updated since
            this time. If not given, defaults to '2010-01-01 00:00:00'

    Returns:
        jobs=[{"job_id": int, "status": str, "display_group": (job group)}]
        time=datetime (use in next request to get jobs that have been updated)
    """
    next_sync = get_time(DB.session)
    DB.session.commit()
    last_sync = request.args.get('last_sync', '2010-01-01 00:00:00')
    display_vals_query = """
    SELECT
        job.job_id,
        job.status,
        job_attribute.value AS display_group
    FROM
        workflow
    JOIN
        job
            ON workflow.dag_id = job.dag_id
    LEFT JOIN
        job_attribute
            ON job.job_id = job_attribute.job_id AND attribute_type = 18
    WHERE
        workflow.id = :workflow_id
        AND job.status_date >= :last_sync
    ORDER BY job.job_id
    """
    res = DB.session.execute(
        display_vals_query,
        {"workflow_id": workflow_id, "last_sync": last_sync}).fetchall()

    # unpack results, discarding the column name in the row_proxies for smaller
    # payloads
    if res:
        jobs = [tuple(res[0].keys())]
        for row_proxy in res:
            row_dict = dict(row_proxy)
            row_dict["status"] = _viz_label_mapping[row_dict["status"]]
            jobs.append(tuple(row_dict.values()))
    else:
        jobs = []

    resp = jsonify(jobs=jobs, time=next_sync)
    resp.status_code = StatusCodes.OK
    return resp


@jvs.route('/workflow_status', methods=['GET'])
def get_workflow_status():
    # convert workflow args into sql filter
    workflow_request = request.args.get('workflow_id', None)
    if workflow_request is not None:
        if workflow_request == "all":
            workflow_filter = ""
        elif isinstance(workflow_request, Iterable):
            workflow_filter = "workflow_id in :workflow_id "
        else:
            workflow_filter = "workflow_id = :workflow_id "
    else:
        workflow_filter = ""

    # convert user args into sql filter
    user_request = request.args.get('user', None)
    if user_request is not None:
        if user_request == "all":
            user_filter = ""
        elif isinstance(user_request, Iterable):
            user_filter = "user in :user "
        else:
            user_filter = "user = :user "
    else:
        user_filter = ""

    # combine filters and build params dictionary
    params = {}
    if workflow_filter or user_filter:
        where_clause = "WHERE "
        if workflow_filter:
            params["workflow_id"] = workflow_request
            where_clause += workflow_filter
        if user_filter:
            where_clause += user_filter
            params["user"] = user_request

    # execute query
    q = """
        SELECT
            workflow.id as WF_ID,
            workflow.name as WF_NAME,
            workflow_status.label as WF_STATUS,
            count(job.status) as JOBS,
            job.STATUS,
            sum(
                CASE
                    WHEN num_attempts <= 1 THEN 0
                    ELSE num_attempts - 1
                END
            ) as RETRIES
        FROM
            workflow
        JOIN job USING (dag_id)
        JOIN workflow_status ON workflow_status.id = workflow.status
        {where_clause}
        GROUP BY workflow.id, job.status
    """.format(where_clause=where_clause)
    res = DB.session.execute(q, params).fetchall()

    if res:

        # assign to dataframe for aggregation
        df = pd.DataFrame(res, columns=res[0].keys())

        # remap to viz statuses
        df.STATUS.replace(to_replace=_viz_label_mapping, inplace=True)

        # pivot wide by job status
        jobs = df.pivot_table(
            values="JOBS",
            index=["WF_ID", "WF_NAME", "WF_STATUS"],
            columns="STATUS",
            fill_value=0)[_viz_order]

        # aggregate totals by workflow
        df = df.groupby(["WF_ID", "WF_NAME", "WF_STATUS"]
                        ).agg({'JOBS': 'sum', 'RETRIES': 'sum'})

        # combine datasets
        df = pd.concat([jobs, df], axis=1)

        # compute pcts and format
        for col in _viz_order:
            df[col + "_pct"] = (
                df[col].astype(float) / df["JOBS"].astype(float)) * 100
            df[col + "_pct"] = df[[col + "_pct"]].round(1)
            df[col] = (
                df[col].astype(int).astype(str) +
                " (" + df[col + "_pct"].astype(str) + "%)")

        # df.replace(to_replace={"0 (0.0%)": "NA"}, inplace=True)
        # final order
        df = df[["JOBS"] + _viz_order + ["RETRIES"]]
        df = df.reset_index()
        df = df.to_json()
        resp = jsonify(workflows=df)
    else:
        df = pd.DataFrame({},
                          columns=["WF_ID", "WF_NAME", "WF_STATUS", "JOBS",
                                   "PENDING", "RUNNING", "DONE", "FATAL",
                                   "RETRIES"]).to_json()
        resp = jsonify(workflows=df)

    resp.status_code = StatusCodes.OK
    return resp
