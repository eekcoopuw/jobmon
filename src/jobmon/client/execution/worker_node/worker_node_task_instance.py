"""The Task Instance Object once it has been submitted to run on a worker node."""
import logging
import os
import socket
import traceback
from typing import Dict, Optional, Tuple, Union

from jobmon.client.client_config import ClientConfig
from jobmon.client.execution.strategies.base import TaskInstanceExecutorInfo
from jobmon.requester import Requester

logger = logging.getLogger(__name__)


class WorkerNodeTaskInstance:
    """The Task Instance object once it has been submitted to run on a worker node."""

    def __init__(self, task_instance_id: int,
                 task_instance_executor_info: TaskInstanceExecutorInfo,
                 requester_url: Optional[str] = None):
        """The WorkerNodeTaskInstance is a mechanism whereby a running task_instance can
        communicate back to the JobStateManager to log its status, errors, usage details, etc.

        Args:
            task_instance_id (int): the id of the job_instance_id that is
                reporting back
            task_instance_executor_info (TaskInstanceExecutorInfo): instance of
                executor that was used for this job instance
            nodename (str): hostname where this job_instance is running
            process_group_id (int): linux process_group_id that this
                job_instance is a part of
            requester_url (str): url to communicate with the flask services.
        """
        self.task_instance_id = task_instance_id
        self._executor_id: Optional[int] = None
        self._nodename: Optional[str] = None
        self._process_group_id: Optional[int] = None
        self.executor = task_instance_executor_info

        if requester_url is None:
            requester_url = ClientConfig.from_defaults().url
        self.requester = Requester(requester_url)

    @property
    def executor_id(self) -> Optional[int]:
        """Executor id given from the executor it is being run on."""
        if self._executor_id is None and self.executor.executor_id is not None:
            self._executor_id = self.executor.executor_id
        logger.debug("executor_id: " + str(self._executor_id))
        return self._executor_id

    @property
    def nodename(self) -> Optional[str]:
        """Node it is being run on."""
        if self._nodename is None:
            self._nodename = socket.getfqdn()
        return self._nodename

    @property
    def process_group_id(self) -> Optional[int]:
        """Process group to track parent and child processes."""
        if self._process_group_id is None:
            self._process_group_id = os.getpid()
        return self._process_group_id

    def log_done(self) -> int:
        """Tell the JobStateManager that this task_instance is done."""
        logger.info(f"Logging done for task_instance {self.task_instance_id}")
        message = {'nodename': self.nodename}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.debug("No executor id was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/log_done',
            message=message,
            request_type='post',
            logger=logger
        )
        return rc

    def log_error(self, error_message: str, exit_status: int) -> int:
        """Tell the JobStateManager that this task_instance has errored."""
        logger.info(f"Logging error for task_instance {self.task_instance_id}")

        # clip at 10k to avoid mysql has gone away errors when posting long
        # messages
        e_len = len(error_message)
        if e_len >= 10000:
            error_message = error_message[-10000:]
            logger.info(f"Error_message is {e_len} which is more than the 10k "
                        "character limit for error messages. Only the final "
                        "10k will be captured by the database.")

        error_state, msg = self.executor.get_exit_info(exit_status, error_message)

        message = {'error_message': msg,
                   'error_state': error_state,
                   'nodename': self.nodename}

        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.debug("No executor_id was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/log_error_worker_node',
            message=message,
            request_type='post',
            logger=logger
        )
        return rc

    def log_task_stats(self) -> None:
        """Tell the JobStateManager all the applicable task_stats for this task_instance."""
        logger.info(f"Logging usage for task_instance {self.task_instance_id}")
        try:
            usage = self.executor.get_usage_stats()
            dbukeys = ['usage_str', 'wallclock', 'maxrss', 'maxpss', 'cpu',
                       'io']
            msg = {k: usage[k] for k in dbukeys if k in usage.keys()}
            rc, _ = self.requester.send_request(
                app_route=f'/worker/task_instance/{self.task_instance_id}/log_usage',
                message=msg,
                request_type='post',
                logger=logger
            )
        except NotImplementedError:
            logger.warning(f"Usage stats not available for "
                           f"{self.executor.__class__.__name__} executors")
        except Exception as e:
            # subprocess.CalledProcessError is raised if qstat fails.
            # Not a critical error, keep running and log an error.
            logger.error(f"Usage stats not available due to exception {e}")
            logger.error(f"Traceback {traceback.format_exc()}")

    def log_running(self, next_report_increment: Union[int, float]) -> Tuple[int, str]:
        """Tell the JobStateManager that this task_instance is running, and update the
        report_by_date to be further in the future in case it gets reconciled immediately.
        """
        logger.info(f'Log running for task_instance {self.task_instance_id}')
        message = {'nodename': self.nodename,
                   'process_group_id': str(self.process_group_id),
                   'next_report_increment': next_report_increment}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.info("No Task ID was found in the qsub env at this time")
        rc, resp = self.requester.send_request(
            app_route=(f'/worker/task_instance/{self.task_instance_id}/log_running'),
            message=message,
            request_type='post',
            logger=logger
        )
        logger.debug(f"Response from log_running was: {resp}")
        return rc, resp

    def log_report_by(self, next_report_increment: Union[int, float]) -> int:
        """Log the heartbeat to show that the task instance is still alive."""
        logger.debug(f"Logging heartbeat for task_instance {self.task_instance_id}")
        message: Dict = {"next_report_increment": next_report_increment}
        if self.executor_id is not None:
            message['executor_id'] = str(self.executor_id)
        else:
            logger.debug("No executor_id was found in the qsub env at this time")
        rc, _ = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/log_report_by',
            message=message,
            request_type='post',
            logger=logger
        )
        return rc

    def in_kill_self_state(self) -> bool:
        """Check if the task instance has been set to kill itself (upon resume or other error
        from miscommunication).
        """
        logger.debug(f"checking kill_self for task_instance {self.task_instance_id}")
        rc, resp = self.requester.send_request(
            app_route=f'/worker/task_instance/{self.task_instance_id}/kill_self',
            message={},
            request_type='get',
            logger=logger
        )
        if resp.get('should_kill'):
            logger.debug("task_instance is in a state that indicates it needs to kill itself")
            return True
        else:
            logger.debug("task instance does not need to kill itself")
            return False
