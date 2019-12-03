from threading import Event, Thread

from jobmon.client.swarm import SwarmLogging as logging
from jobmon.client.swarm.job_management.task_instance_factory import \
    TaskInstanceFactory
from jobmon.client.swarm.job_management.task_instance_reconciler import \
    TaskInstanceReconciler

logger = logging.getLogger(__name__)


class TaskInstanceStateController(object):
    def __init__(self, workflow_id, workflow_run_id, executor=None,
                 start_daemons=False, task_instantiation_interval=10,
                 n_queued_jobs=1000):
        """
        Once a task is ready to be executed, it is passed off to the task
        instance state controller to create task instances and handles all of
        the state monitoring and external changing that a task instance cannot
        do for itself

        :param workflow_id(int): The id for the workflow to be referenced by
        :param executor (obj): Which executor the tasks are running on
        :param start_daemons (bool): If the TaskInstanceReconciler and
            TaskInstanceFactory should be started in daemonized threads
        :param task_instantiation_interval (int, default 10): number of seconds
            to wait between instantiating newly ready tasks
        :param n_queued_jobs (int): number of queued tasks that should be
            returned to be instantiated
        """
        self._stop_event = Event()
        self.job_instance_factory = TaskInstanceFactory(
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            executor=executor,
            n_queued_tasks=n_queued_jobs,
            stop_event=self._stop_event)

        self.job_instance_reconciler = TaskInstanceReconciler(
            workflow_run_id=workflow_run_id,
            executor=executor,
            stop_event=self._stop_event)
        self.jif_process = None
        self.jir_process = None

        self.job_instantiation_interval = task_instantiation_interval
        if start_daemons:
            self.run_scheduler()

    def run_scheduler(self):
        """Start the Job Instance Factory and Job Instance Reconciler in
        separate threads"""
        self.instantiate_queued_jobs()
        self.reconcile()

    def instantiate_queued_jobs(self):
        self.jif_process = Thread(
            target=(
                self.job_instance_factory.instantiate_queued_tasks_periodically
            ),
            args=(self.job_instantiation_interval,))
        self.jif_process.daemon = True
        self.jif_process.start()

    def reconcile(self):
        self.jir_process = Thread(
            target=self.job_instance_reconciler.reconcile_periodically)
        self.jir_process.daemon = True
        self.jir_process.start()

    def connect(self):
        self._stop_event = Event()
        self.run_scheduler()

    def disconnect(self):
        self._stop_event.set()
