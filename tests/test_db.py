from time import sleep

from jobmon.models.task_dag import TaskDagMeta
from jobmon.models.job import Job
from jobmon.models.job_status import JobStatus
from jobmon.models.workflow_run import WorkflowRun as WorkflowRunDAO
from jobmon.models.workflow_run_status import WorkflowRunStatus
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_status import WorkflowStatus


def test_job_submit_times(db_cfg):
    """Test that db datetimes aren't all the same..."""
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    with app.app_context():

        # Create dags
        dag = TaskDagMeta(dag_hash='abcd', name='foo', user='bar')
        DB.session.add(dag)
        DB.session.commit()

        sleep(1)

        dag2 = TaskDagMeta(dag_hash='abcd2', name='foo2', user='bar')
        DB.session.add(dag2)
        DB.session.commit()

        dag_id = dag.dag_id

        # Create workflows
        wf1 = Workflow(dag_id=dag_id, status=WorkflowStatus.CREATED)
        DB.session.add(wf1)
        DB.session.commit()
        sleep(1)
        wf2 = Workflow(dag_id=dag_id, status=WorkflowStatus.CREATED)
        DB.session.add(wf2)
        DB.session.commit()

        # And runs...
        wfr1 = WorkflowRunDAO(workflow_id=wf1.id,
                              status=WorkflowRunStatus.RUNNING)
        DB.session.add(wfr1)
        DB.session.commit()
        sleep(1)
        wfr2 = WorkflowRunDAO(workflow_id=wf1.id,
                              status=WorkflowRunStatus.RUNNING)
        DB.session.add(wfr2)
        DB.session.commit()

        # Create a job
        job1 = Job(dag_id=dag_id, name='test1', job_hash=1,
                   status=JobStatus.REGISTERED)
        DB.session.add(job1)
        DB.session.commit()

    sleep(1)
    with app.app_context():
        job2 = Job(dag_id=dag_id, name='test2', job_hash=2,
                   status=JobStatus.REGISTERED)
        DB.session.add(job2)
        DB.session.commit()

    sleep(1)
    with app.app_context():
        job3 = Job(dag_id=dag_id, name='test3', job_hash=3,
                   status=JobStatus.REGISTERED)
        DB.session.add(job3)
        DB.session.commit()

    with app.app_context():
        dags = DB.session.query(TaskDagMeta).all()
        wfs = DB.session.query(Workflow).all()
        wfrs = DB.session.query(WorkflowRunDAO).all()
        jobs = DB.session.query(Job).all()

        assert len(dags) == 2
        assert len(wfs) == 2
        assert len(wfrs) == 2
        assert len(jobs) == 3

        # Ensure the creation/submission dates for each
        # entry are different
        assert len(set([d.created_date for d in dags])) == 2
        assert len(set([wf.created_date for wf in wfs])) == 2
        assert len(set([wfr.created_date for wfr in wfrs])) == 2
        assert len(set([j.submitted_date for j in jobs])) == 3
