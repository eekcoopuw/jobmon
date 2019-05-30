import pytest

from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.task_dag import DagExecutionStatus
from jobmon.models.attributes.constants import workflow_run_attribute


def test_workflow_run_attribute(real_jsm_jqs, db_cfg):
    # create a workflow_run
    wfa = "test_workflow_run_attribute"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    # add an attribute to workflow_run
    workflow_run.add_workflow_run_attribute(workflow_run_attribute.NUM_DRAWS,
                                            "1000")

    # query from workflow_run_attribute table
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        attribute_query = DB.session.execute("""
            SELECT wf_run_att.id,
                   wf_run_att.workflow_run_id,
                   wf_run_att.attribute_type,
                   wf_run_att.value
            FROM workflow_run_attribute
                 as wf_run_att
            JOIN workflow_run as wf_run
            ON wf_run_att.workflow_run_id
               =wf_run.id
            WHERE wf_run_att.workflow_run_id
                  ={id}
            AND wf_run_att.attribute_type = {ty}
            """.format(id=workflow_run.id, ty=workflow_run_attribute.NUM_DRAWS))

        attribute_entry = attribute_query.fetchone()
        entry_type = attribute_entry.attribute_type
        entry_value = attribute_entry.value
        DB.session.commit()

        assert entry_type == workflow_run_attribute.NUM_DRAWS
        assert entry_value == "1000"


def test_workflow_run_attribute_input_error(real_jsm_jqs, db_cfg):
    # create a workflow_run
    wfa = "test_workflow_run_attribute_input_error"
    workflow = Workflow(wfa)
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    # add an attribute with wrong types to the workflow_run
    with pytest.raises(ValueError) as exc:
        workflow_run.add_workflow_run_attribute("num_draws", "ten")
    assert "Invalid" in str(exc.value)


@pytest.mark.qsubs_jobs
def test_new_workflow_has_project_limit(real_jsm_jqs, db_cfg):
    wfa = "test_new_workflow_has_project_limit"
    workflow = Workflow(wfa, project='proj_tools')
    t1 = BashTask("sleep 1", num_cores=1)
    workflow.add_tasks([t1])
    workflow._bind()
    workflow._create_workflow_run()
    workflow_run = workflow.workflow_run

    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # query from workflow_run_attribute table
        attribute_query = DB.session.execute("""
                                SELECT wf_run_att.id,
                                       wf_run_att.workflow_run_id,
                                       wf_run_att.attribute_type,
                                       wf_run_att.value
                                FROM workflow_run_attribute
                                     as wf_run_att
                                JOIN workflow_run as wf_run
                                ON wf_run_att.workflow_run_id
                                   =wf_run.id
                                WHERE wf_run_att.workflow_run_id
                                      ={id}
                                """.format(id=workflow_run.id))

        attribute_entry = attribute_query.fetchone()
        entry_type = attribute_entry.attribute_type
        entry_value = attribute_entry.value

        assert entry_type == workflow_run_attribute.SLOT_LIMIT_AT_START
        assert entry_value  # Can't be None, although it could be -1 if no
        # slot limits

        # advance to done state
        dag_status, n_new_done, n_prev_done, n_failed = (
            workflow.task_dag._execute_interruptible())
        if dag_status == DagExecutionStatus.SUCCEEDED:
            workflow._done()
        elif dag_status == DagExecutionStatus.FAILED:
            workflow._error()
        elif dag_status == DagExecutionStatus.STOPPED_BY_USER:
            workflow._stopped()
        else:
            raise RuntimeError("Received unknown response from "
                               "TaskDag._execute()")

    with app.app_context():
        # make sure SLOT_LIMIT_AT_END  is filled in
        # query from workflow_run_attribute table
        attribute_query = DB.session.execute("""
                                SELECT wf_run_att.id,
                                       wf_run_att.workflow_run_id,
                                       wf_run_att.attribute_type,
                                       wf_run_att.value
                                FROM workflow_run_attribute
                                     as wf_run_att
                                JOIN workflow_run as wf_run
                                ON wf_run_att.workflow_run_id
                                   =wf_run.id
                                WHERE wf_run_att.workflow_run_id
                                      ={id}
                                AND wf_run_att.attribute_type={t}
                                """.format(
            id=workflow_run.id, t=workflow_run_attribute.SLOT_LIMIT_AT_END))

        attribute_entry = attribute_query.fetchone()
        entry_type = attribute_entry.attribute_type
        entry_value = attribute_entry.value

        assert entry_type == workflow_run_attribute.SLOT_LIMIT_AT_END
        assert entry_value  # Can't be None, although it could be -1 if no
        # slot limits
