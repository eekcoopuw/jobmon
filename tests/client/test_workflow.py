import pytest


# @pytest.mark.qsubs_jobs
# def test_wfargs_update(env_var, db_cfg):
#     # Create identical dags
#     t1 = BashTask("sleep 1", num_cores=1)
#     t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
#     t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

#     t4 = BashTask("sleep 1", num_cores=1)
#     t5 = BashTask("sleep 2", upstream_tasks=[t4], num_cores=1)
#     t6 = BashTask("sleep 3", upstream_tasks=[t5], num_cores=1)

#     wfa1 = "v1"
#     wf1 = Workflow(wfa1)
#     wf1.add_tasks([t1, t2, t3])
#     wf1.execute()

#     wfa2 = "v2"
#     wf2 = Workflow(wfa2)
#     wf2.add_tasks([t4, t5, t6])
#     wf2.execute()

#     # Make sure the second Workflow has a distinct Workflow ID & WorkflowRun ID
#     assert wf1.id != wf2.id

#     # Make sure the second Workflow has a distinct hash
#     assert wf1.hash != wf2.hash

#     # Make sure the second Workflow has a distinct set of Jobs
#     assert not (set([t.job_id for _, t in wf1.task_dag.bound_tasks.items()]) &
#                 set([t.job_id for _, t in wf2.task_dag.bound_tasks.items()]))


# @pytest.mark.qsubs_jobs
# def test_attempt_resume_on_complete_workflow(simple_workflow):
#     """Should not allow a resume, but should prompt user to create a new
#     workflow by modifying the WorkflowArgs (e.g. new version #)
#     """
#     # Re-create the dag "from scratch" (copy simple_workflow fixture)
#     t1 = BashTask("sleep 1", num_cores=1)
#     t2 = BashTask("sleep 2", upstream_tasks=[t1], num_cores=1)
#     t3 = BashTask("sleep 3", upstream_tasks=[t2], num_cores=1)

#     wfa = "my_simple_dag"
#     workflow = Workflow(wfa, resume=ResumeStatus.RESUME)
#     workflow.add_tasks([t1, t2, t3])

#     with pytest.raises(WorkflowAlreadyComplete):
#         workflow.execute()


# def test_workflow_identical_args(env_var, db_cfg):
#     # first workflow runs and finishes
#     wf1 = Workflow(workflow_args="same", project='proj_tools')
#     task = BashTask("sleep 2", num_cores=1)
#     wf1.add_task(task)
#     wf1.execute()

#     # tries to create an identical workflow without the restart flag
#     wf2 = Workflow(workflow_args="same", project='proj_tools')
#     wf2.add_task(task)
#     with pytest.raises(WorkflowAlreadyExists):
#         wf2.execute()

#     # creates a workflow, okayed to restart, but original workflow is done
#     wf3 = Workflow(workflow_args="same", project='proj_tools',
#                    resume=ResumeStatus.RESUME)
#     wf3.add_task(task)
#     with pytest.raises(WorkflowAlreadyComplete):
#         wf3.execute()


# def test_same_wf_args_diff_dag(env_var, db_cfg):
#     wf1 = Workflow(workflow_args="same", project='proj_tools')
#     task1 = BashTask("sleep 2", num_cores=1)
#     wf1.add_task(task1)

#     wf2 = Workflow(workflow_args="same", project='proj_tools')
#     task2 = BashTask("sleep 3", num_cores=1)
#     wf2.add_task(task2)

#     exit_status = wf1.execute()

#     assert exit_status == 0

#     with pytest.raises(WorkflowAlreadyExists):
#         wf2.run()
