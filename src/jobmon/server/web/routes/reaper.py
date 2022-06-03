"""Routes used to move through the finite state."""
from flask import Blueprint

blueprint = Blueprint('reaper', __name__)




@blueprint.route(
    "workflow/<workflow_id>/fix_status_inconsistency", methods=["PUT"]
)
def fix_wf_inconsistency(workflow_id: int) -> Any:
    """Find wf in F with all tasks in D and fix them.

    For flexibility, pass in the step size. It is easier to redeploy the reaper than the
    service.
    """
    data = request.get_json()
    increase_step = data["increase_step"]

    bind_to_logger(workflow_id=workflow_id)
    logger.debug(f"Fix inconsistencies starting at workflow {workflow_id} by {increase_step}")
    sql = "SELECT COUNT(*) as total FROM workflow"
    # the id to return to reaper as next start point
    total_wf = int(DB.session.execute(sql).fetchone()["total"])

    # move the starting row forward by increase_step
    # It takes about 1 second per thousand; increase_step is passed in from the reaper.
    # Sf the starting row > max row, restart from workflow-id 0.
    # This way, we can get to the unfinished the wf later
    # without querying the whole db every time.

    current_max_wf_id = int(workflow_id) + int(increase_step)
    if current_max_wf_id > total_wf:
        logger.info("Fix inconsistencies starting from workflow_id zero again")
        current_max_wf_id = 0

    # Update wf in F with all task in D to D
    # count(s) will have the total number of tasks, sum(s) is those in D.
    # If the two are equal, then the workflow Tasks are all D and therefore the workflow
    # should be D.

    query_sql = """
            SELECT id
                    FROM
                        (SELECT workflow.id, (case when task.status="D" then 1 else 0 end) as s
                        FROM workflow, task
                        WHERE workflow.id > {low_wfid}
                        AND workflow.id <= {high_wfid}
                        AND workflow.status='F'
                        AND workflow.id=task.workflow_id) t
                        GROUP BY id
                        HAVING count(s) = sum(s)
            """.format(
        low_wfid=workflow_id, high_wfid=int(workflow_id) + increase_step
    )

    result_list = DB.session.execute(query_sql).fetchall()
    DB.session.commit()
    if result_list is None or len(result_list) == 0:
        logger.debug("No inconsistent F-D workflows to fix.")
    else:
        ids = ""
        for row in result_list:
            ids += f"{row['id']},"
        # strip last comma
        ids = ids[:-1]
        logger.info("Fixing inconsistent F-D workflow: {ids}")
        update_sql = f"""UPDATE workflow
                        SET status = "D"
                        WHERE id IN ({ids})
                        """
        DB.session.execute(update_sql)
        DB.session.commit()
        logger.debug("Done fixing F-D inconsistent workflows.")

    resp = jsonify({"wfid": current_max_wf_id})
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route(
    "workflow/<workflow_id>/workflow_name_and_args", methods=["GET"]
)
def get_wf_name_and_args(workflow_id: int) -> Any:
    """Return workflow name and args associated with specified workflow ID."""
    query = f"""
        SELECT
            workflow.name as workflow_name, workflow.workflow_args as workflow_args
        FROM
            workflow
        WHERE
            workflow.id = {workflow_id}
    """
    result = DB.session.execute(query).fetchone()
    if result is None:
        # return empty values in case of DB inconsistency
        resp = jsonify(workflow_name=None, workflow_args=None)
        resp.status_code = StatusCodes.OK
        return resp

    resp = jsonify(
        workflow_name=result["workflow_name"], workflow_args=result["workflow_args"]
    )
    resp.status_code = StatusCodes.OK
    return resp


@blueprint.route("/lost_workflow_run", methods=["GET"])
def get_lost_workflow_runs() -> Any:
    """Return all workflow runs that are currently in the specified state."""
    statuses = request.args.getlist("status")
    version = request.args.get("version")
    query = """
        SELECT
            workflow_run.*
        FROM
            workflow_run
        WHERE
            workflow_run.status in :workflow_run_status
            and workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
            and workflow_run.jobmon_version = :version
    """
    workflow_runs = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_status=statuses, version=version)
        .all()
    )
    DB.session.commit()
    workflow_runs = [wfr.to_wire_as_reaper_workflow_run() for wfr in workflow_runs]
    resp = jsonify(workflow_runs=workflow_runs)
    resp.status_code = StatusCodes.OK
    return resp



@blueprint.route("/workflow_run/<workflow_run_id>/reap", methods=["PUT"])
def reap_workflow_run(workflow_run_id: int) -> Any:
    """If the last task was more than 2 minutes ago, transition wfr to A state.

    Also check WorkflowRun status_date to avoid possible race condition where reaper
    checks tasks from a different WorkflowRun with the same workflow id. Avoid setting
    while waiting for a resume (when workflow is in suspended state).
    """
    structlog.threadlocal.bind_threadlocal(workflow_run_id=workflow_run_id)
    logger.info(f"Reap wfr: {workflow_run_id}")
    query = """
        SELECT
            workflow_run.*
        FROM workflow_run
        WHERE
            workflow_run.id = :workflow_run_id
            and workflow_run.heartbeat_date <= CURRENT_TIMESTAMP()
    """
    wfr = (
        DB.session.query(WorkflowRun)
        .from_statement(text(query))
        .params(workflow_run_id=workflow_run_id)
        .one_or_none()
    )
    DB.session.commit()

    try:
        wfr.reap()
        DB.session.commit()
        status = wfr.status
    except (InvalidStateTransition, AttributeError) as e:
        # this branch handles race condition or case where no wfr was returned
        logger.debug(f"Unable to reap workflow_run {wfr.id}: {e}")
        status = ""
    resp = jsonify(status=status)
    resp.status_code = StatusCodes.OK
    return resp
