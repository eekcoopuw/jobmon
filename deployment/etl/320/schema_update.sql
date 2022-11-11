ALTER TABLE 'workflow_run'
ADD COLUMN 'jobmon_server_version' VARCHAR(150) NULL DEFAULT NULL AFTER 'jobmon_version';

# Adding descriptions to the state tables
# The four alter table statements need to be run by user "root"
# The inserts only need service_user

#----------------------------------------------------------------------
# Add descriptions to the workflow_status table

ALTER TABLE workflow_status
ADD COLUMN description VARCHAR(150) DEFAULT '' AFTER label;


UPDATE workflow_status
SET description = 'Workflow encountered an error before a WorkflowRun was created.'
WHERE id ='A';

UPDATE workflow_status
SET description = 'Workflow is Done, it finished successfully.'
WHERE id ='D';

UPDATE workflow_status
SET description = 'Workflow unsuccessful in one or more WorkflowRuns, no runs finished successfully as DONE.'
WHERE id ='F';

UPDATE workflow_status
SET description = 'Workflow is being validated.'
WHERE id ='G';

UPDATE workflow_status
SET description = 'Resume was set and Workflow is shut down or the controller died and therefore Workflow was reaped.'
WHERE id ='H';

UPDATE workflow_status
SET description = 'Jobmon Scheduler is creating a Workflow on the distributor.'
WHERE id ='I';

UPDATE workflow_status
SET description = 'Workflow has been created. Distributor is now controlling tasks, or waiting for scheduling loop.'
WHERE id ='O';

UPDATE workflow_status
SET description = 'Jobmon client has updated the Jobmon database, and signalled Scheduler to create Workflow.'
WHERE id ='Q';

UPDATE workflow_status
SET description = 'Workflow has a WorkflowRun that is running.'
WHERE id ='R';


#----------------------------------------------------------------------
# Add descriptions to the workflow_run_status table
ALTER TABLE workflow_run_status
ADD COLUMN description VARCHAR(150) DEFAULT '' AFTER label;


UPDATE workflow_run_status
SET description = 'WorkflowRun encountered problems while binding so it stopped.'
WHERE id ='A';

UPDATE workflow_run_status
SET description = 'WorkflowRun has been bound to the database.'
WHERE id ='B';

UPDATE workflow_run_status
SET description = 'WorkflowRun is set to resume as soon all existing tasks are killed.'
WHERE id ='C';

UPDATE workflow_run_status
SET description = 'WorkflowRun is Done, it successfully completed.'
WHERE id ='D';

UPDATE workflow_run_status
SET description = 'WorkflowRun did not complete successfully, perhaps lost contact with services.'
WHERE id ='E';

UPDATE workflow_run_status
SET description = 'WorkflowRun has been validated.'
WHERE id ='G';

UPDATE workflow_run_status
SET description = 'WorkflowRun was set to hot-resume while tasks are still running, they will continue running.'
WHERE id ='H';

UPDATE workflow_run_status
SET description = 'Scheduler is instantiating a WorkflowRun on the distributor.'
WHERE id ='I';

UPDATE workflow_run_status
SET description = 'WorkflowRun completed successfully, updating the Workflow.'
WHERE id ='L';

UPDATE workflow_run_status
SET description = 'Instantiation complete. Distributor is controlling Tasks or waiting for scheduling loop.'
WHERE id ='O';

UPDATE workflow_run_status
SET description = 'WorkflowRun is currently running.'
WHERE id ='R';

UPDATE workflow_run_status
SET description = 'WorkflowRun was deliberately stopped, probably due to keyboard interrupt from user.'
WHERE id ='S';

UPDATE workflow_run_status
SET description = 'This WorkflowRun is being replaced by a new WorkflowRun created to pick up remaining Tasks, this WFR is terminating.'
WHERE id ='T';

#----------------------------------------------------------------------

# Add descriptions to the task_status table
ALTER TABLE task_status
ADD COLUMN description VARCHAR(150) DEFAULT '' AFTER label;

UPDATE task_status
SET description = 'Task errored with a resource error, the resources will be adjusted before retrying.'
WHERE id ='A';

UPDATE task_status
SET description = 'Task is Done, it ran successfully to completion; it has a TaskInstance that successfully completed.'
WHERE id ='D';

UPDATE task_status
SET description = 'Task has errored out but has more attempts so it will be retried.'
WHERE id ='E';

UPDATE task_status
SET description = 'Task errored out and has used all of the attempts, therefore has failed permanently. It cannot be retried.'
WHERE id ='F';

UPDATE task_status
SET description = 'Task is bound to the database.'
WHERE id ='G';

UPDATE task_status
SET description = 'Task is created within Jobmon.'
WHERE id ='I';

# LAUNCHED in 3.2.0
# Missing state O
INSERT into task_instance_status(id, label, description)
VALUES ('O', 'LAUNCHED', description = 'Task instance submitted to the cluster normally, part of a Job Array.' );

UPDATE task_status
SET description = 'Task''s dependencies have successfully completed, task can be run when the scheduler is ready.'
WHERE id ='Q';

UPDATE task_status
SET description = 'Task is running on the specified distributor.'
WHERE id ='R';


#----------------------------------------------------------------------

# Add descriptions to the task_instance_status table
ALTER TABLE task_instance_status
ADD COLUMN description VARCHAR(150) DEFAULT '' AFTER label;

# Not 3.2.0
UPDATE task_instance_status
SET description = 'Task instance submitted to the cluster normally.'
WHERE id ='B';

UPDATE task_instance_status
SET description = 'Task instance finished successfully.'
WHERE id ='D';

UPDATE task_instance_status
SET description = 'Task instance stopped with an application error (non-zero return code).'
WHERE id ='E';

UPDATE task_instance_status
SET description = 'Task instance killed itself as part of a cold workflow resume, and cannot be retried.'
WHERE id ='F';

UPDATE task_instance_status
SET description = 'Task instance is created within Jobmon, but not queued for submission to the cluster.'
WHERE id ='I';

UPDATE task_instance_status
SET description = 'Task instance has been ordered to kill itself if it is still alive, as part of a cold workflow resume.'
WHERE id ='K';

# LAUNCHED in 3.2.0
UPDATE task_instance_status
SET description = 'Task instance submitted to the cluster normally, part of a Job Array.'
WHERE id ='O';

# Missing state Q
INSERT into task_instance_status(id, label, description)
VALUES ('Q', 'QUEUED', 'TaskInstance is queued for submission to the cluster.');

UPDATE task_instance_status
SET description = 'Task instance has started running normally.'
WHERE id ='R';

# Missing state T
INSERT into task_instance_status(id, label, description)
VALUES ('T', 'TRIAGING', 'Task instance has errored, Jobmon is determining the category of error.' );

UPDATE task_instance_status
SET description = 'Task instance stopped reporting that it was alive for an unknown reason.'
WHERE id ='U';

UPDATE task_instance_status
SET description = 'Task instance submission within Jobmon failed – did not receive a distributor_id from the cluster.'
WHERE id ='W';

UPDATE task_instance_status
SET description = 'Task instance died because of insufficient resource request, i.e. insufficient memory or runtime.'
WHERE id ='Z';