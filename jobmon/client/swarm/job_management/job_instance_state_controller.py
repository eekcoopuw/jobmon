from threading import Event, Thread

from jobmon.client.client_logging import ClientLogging as logging
from jobmon.client.swarm.job_management.job_instance_factory import JobInstanceFactory
from jobmon.client.swarm.job_management.job_instance_reconciler import JobInstanceReconciler

logger = logging.getLogger(__name__)


class JobInstanceStateController(object):
    def __init__(self, dag_id, executor=None, start_daemons=False,
                 job_instantiation_interval=10, n_queued_jobs=1000):
        """
        Once a job is ready to be executed, it is passed off to the job
        instance state controller to create job instances and handles all of
        the state monitoring and external changing that a job instance cannot
        do for itself

        :param dag_id(int): The dag id for the workflow run to be referenced by
        :param executor (obj): Which executor the jobs are running on
        :param start_daemons (bool): If the JobInstanceReconciler and
            JobInstanceFactory should be started in daemonized threads
        :param job_instantiation_interval (int, default 10): number of seconds
            to wait between instantiating newly ready jobs
        :param n_queued_jobs (int): number of queued jobs that should be
            returned to be instantiated
        """
        self.dag_id = dag_id
        self._stop_event = Event()
        self.job_instance_factory = JobInstanceFactory(
            dag_id=dag_id,
            executor=executor,
            n_queued_jobs=n_queued_jobs,
            stop_event=self._stop_event)

        self.job_instance_reconciler = JobInstanceReconciler(
            dag_id=dag_id,
            executor=executor,
            stop_event=self._stop_event)
        self.jif_process = None
        self.jir_process = None

        self.job_instantiation_interval = job_instantiation_interval
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
                self.job_instance_factory.instantiate_queued_jobs_periodically
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