import datetime

from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.client import client_config


class SwarmJobInstance:

    def __init__(self, job_instance_id, executor_id, executor,
                 workflow_run_id=None, nodename=None, process_group_id=None,
                 job_id=None, dag_id=None, status=None, status_date=None):

        # job_instance_id should be immutable so make it private
        self.job_instance_id = job_instance_id
        self.executor_id = executor_id

        # interface to the executor
        self.executor = executor

        # these attributes do not affect any current functionality but are
        # returned from the job_query_service
        self.workflow_run_id = workflow_run_id
        self.nodename = nodename
        self.process_group_id = process_group_id
        self.job_id = job_id
        self.dag_id = dag_id
        self.status = status
        self.status_date = status_date

    @classmethod
    def from_wire(cls, dct, executor):
        return cls(job_instance_id=dct['job_instance_id'],
                   executor=executor,
                   workflow_run_id=dct['workflow_run_id'],
                   executor_id=dct['executor_id'],
                   nodename=dct['nodename'],
                   process_group_id=dct['process_group_id'],
                   job_id=dct['job_id'],
                   dag_id=dct['dag_id'],
                   status=dct['status'],
                   status_date=datetime.strptime(dct['status_date'],
                                                 "%Y-%m-%dT%H:%M:%S"))

    def time_since_status(self):
        return (datetime.utcnow() - self.status_date).seconds

    def log_error(self):
        error_state, msg = self.executor.get_remote_exit_info(self.executor_id)

        # this is the 'happy' path. The executor gives us a concrete error for
        # the lost job
        if error_state == JobInstanceStatus.RESOURCE_ERROR:
            message = {
                "error_message": msg,
                "error_state": error_state,
                "executor_id": self.executor_id
            }
        # this is the 'unhappy' path. We are giving up discovering the exit
        # state and moving the job into unknown error state
        elif self.time_since_status > client_config.lost_track_timeout:
            message = {
                "error_message": msg,
                "error_state": error_state
            }
        # this is the do nothing path. We want to wait longer to see if the
        # executor discovers the real exit state before giving up on the job
        else:
            return
        self.requester.send_request(
            app_route=f'/job_instance/{self.job_instance_id}/log_error',
            message=message,
            request_type='post')
