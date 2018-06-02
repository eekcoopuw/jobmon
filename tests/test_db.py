from time import sleep

from jobmon.database import session_scope
from jobmon.meta_models.task_dag import TaskDagMeta
from jobmon.models import Job, JobStatus
from jobmon.workflow.workflow import WorkflowDAO, WorkflowStatus
from jobmon.workflow.workflow_run import WorkflowRunDAO, WorkflowRunStatus


def test_job_submit_times(db_cfg):
    """Test that db datetimes aren't all the same..."""
    with session_scope() as session:

        # Create dags
        dag = TaskDagMeta(dag_hash='abcd', name='foo', user='bar')
        session.add(dag)
        session.commit()

        sleep(1)

        dag2 = TaskDagMeta(dag_hash='abcd2', name='foo2', user='bar')
        session.add(dag2)
        session.commit()

        dag_id = dag.dag_id

        # Create workflows
        wf1 = WorkflowDAO(dag_id=dag_id, status=WorkflowStatus.CREATED)
        session.add(wf1)
        session.commit()
        sleep(1)
        wf2 = WorkflowDAO(dag_id=dag_id, status=WorkflowStatus.CREATED)
        session.add(wf2)
        session.commit()

        # And runs...
        wfr1 = WorkflowRunDAO(workflow_id=wf1.id,
                              status=WorkflowRunStatus.RUNNING)
        session.add(wfr1)
        session.commit()
        sleep(1)
        wfr2 = WorkflowRunDAO(workflow_id=wf1.id,
                              status=WorkflowRunStatus.RUNNING)
        session.add(wfr2)
        session.commit()

        # Create a job
        job1 = Job(dag_id=dag_id, name='test1', status=JobStatus.REGISTERED)
        session.add(job1)
        session.commit()

    sleep(1)
    with session_scope() as session:
        job2 = Job(dag_id=dag_id, name='test2', status=JobStatus.REGISTERED)
        session.add(job2)
        session.commit()

    sleep(1)
    with session_scope() as session:
        job3 = Job(dag_id=dag_id, name='test3', status=JobStatus.REGISTERED)
        session.add(job3)
        session.commit()

    with session_scope() as session:

        dags = session.query(TaskDagMeta).all()
        wfs = session.query(WorkflowDAO).all()
        wfrs = session.query(WorkflowRunDAO).all()
        jobs = session.query(Job).all()

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
