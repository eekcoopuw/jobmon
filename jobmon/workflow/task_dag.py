import functools
import getpass
import hashlib
import logging
import copy
from collections import OrderedDict

from jobmon.job_instance_factory import execute_sge
from jobmon.job_list_manager import JobListManager
from jobmon.models import JobStatus
from jobmon.workflow.task_dag_factory import TaskDagMetaFactory

logger = logging.getLogger(__name__)


class TaskDag(object):
    """
    A DAG of Tasks.
    """

    def __init__(self, name=""):

        self.dag_id = None

        # TODO: Scale test to 1M jobs
        self.name = name
        self.tasks = OrderedDict()  # dict for scalability to 1,000,000+ jobs
        self.top_fringe = []
        self.fail_after_n_executions = None

    def _set_fail_after_n_executions(self, n):
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production. """
        self.fail_after_n_executions = n

    @property
    def is_bound(self):
        """Boolean indicating whether the Dag is bound to the DB"""
        if self.dag_id:
            return True
        else:
            return False

    def bind_to_db(self, dag_id=None, executor_args={}):
        """Binds the dag to the database and starts Job Management services.

        Args:
            dag_id (int): Defaults to None, in which case a new dag_id is
                created
        """

        # TODO: executor is expected to be a callable... so just pass the
        # executor args as a partial. in future, we may want to formalize
        # Executor as a class of its own
        executor = functools.partial(execute_sge, **executor_args)
        executor.__name__ = execute_sge.__name__

        if dag_id:
            self.job_list_manager = JobListManager(dag_id,
                                                   executor=executor,
                                                   start_daemons=True)

            # Reset any jobs hung up in not-DONE states
            self.job_list_manager.reset_jobs()

            # Sync task statuses via job_id... consider using hashes instead
            for _, task in self.tasks.items():
                job_id = self.job_list_manager.job_hash_id_map[str(task.hash)]
                task.job_id = job_id
                task.status = self.job_list_manager.job_statuses[job_id]
            self.dag_id = dag_id
        else:
            tdf = TaskDagMetaFactory()
            self.meta = tdf.create_task_dag(name=self.name, dag_hash=self.hash,
                                            user=getpass.getuser())
            self.job_list_manager = JobListManager(self.meta.dag_id,
                                                   executor=executor,
                                                   start_daemons=True)
            self.dag_id = self.meta.dag_id
            for _, task in self.tasks.items():
                task.bind(self.job_list_manager)

    def validate(self, raises=True):
        """
        Mostly useful for testing, but also for debugging, and also as a solid
        example of how this complex data structure should look.

        Check:
          - Only one graph - not two sub graphs TBD
          - External Tasks cannot have upstreams (Future releases, when there
          - TBD

          Args:
                raises (boolean): If True then raise ValueError if it detects
                a problem, otherwise return False and log

        Returns:
                True if no problems. if raises is True, then it raises
                ValueError on first problem, else False
        """

        # The empty graph cannot have errors
        if len(self.tasks):
            return True

        error_message = None

        # TBD Is it worth keeping NetworkX just to check it is a DAG?
        # if not nx.is_directed_acyclic_graph(self.task_graph):
        #     error_message = "The graph is not a DAG"
        if not self.top_fringe:
            error_message = "The graph has no top-fringe, an algorithm bug."

        if error_message:
            if raises:
                raise ValueError(error_message)
            else:
                logger.warning(error_message)
                return False
        return True

    def execute(self):
        """
        Take a concrete DAG and queue all the Tasks that are not DONE.

        Uses forward chaining from initial fringe, hence out-of-date is not
        applied transitively backwards through the graph. It could also use
        backward chaining from an identified goal node, the effect is
        dentical.

        The internal data structures are lists, but might need to be changed to
        be better at scaling.

        Conceptually:
        Mark all Tasks as not tried for this execution
        while the fringe is not empty:
          if the job is DONE, skip it and add its downstreams to the fringe
          if not, queue it
          wait for some jobs to complete
          rinse and repeat

        Returns:
            A triple: True, len(all_completed_tasks), len(all_failed_tasks)
        """

        if not self.is_bound:
            self.bind_to_db()
        self._set_top_fringe()

        logger.debug("self.fail_after_n_executions is {}"
                     .format(self.fail_after_n_executions))
        fringe = copy.copy(self.top_fringe)

        all_completed = []
        all_failed = []
        all_running = {}
        already_done = [task for task in self.tasks.values()
                        if task.status == JobStatus.DONE]
        n_executions = 0

        logger.debug("Execute DAG {}".format(self))
        # These are all Tasks.
        # While there is something ready to be run, or something is running
        while fringe or all_running:
            # Everything in the fringe should be run or skipped,
            # they either have no upstreams, or all upstreams are marked DONE
            # in this execution

            while fringe:
                # Get the front of the queue, add to the end.
                # That ensures breadth-first behavior, which is likely to
                # maximize parallelism
                task = fringe.pop(0)
                # Start the new jobs ASAP
                if not task.is_done():
                    logger.debug("Queueing newly ready task {}".format(task))
                    task.queue_job(self.job_list_manager)
                    all_running[task.job_id] = task
                else:
                    raise RuntimeError("Invalid DAG. Encountered a DONE node.")

            # TBD timeout?
            completed_and_status = (
                self.job_list_manager.block_until_any_done_or_error())
            for job in completed_and_status:
                if job[1] == JobStatus.DONE:
                    n_executions += 1
            logger.debug("Return from blocking call, completed_and_status {}"
                         .format(completed_and_status))
            all_running, completed_tasks, failed_tasks = (
                self.sort_jobs(all_running, completed_and_status))

            # Need to find the tasks that were that job, they will be in this
            # "small" dic of active tasks

            all_completed += completed_tasks
            all_failed += failed_tasks
            for task in completed_tasks:
                task_to_add = self.propagate_results(task)
                fringe = list(set(fringe+task_to_add))
            if (self.fail_after_n_executions is not None and
                    n_executions >= self.fail_after_n_executions):
                raise ValueError("Dag asked to fail after {} executions. "
                                 "Failing now".format(n_executions))

        # END while fringe or all_running

        # To be a dynamic-DAG  tool, we must be prepared for the DAG to have
        # changed. In general we would recompute forward from the the fringe.
        # Not efficient, but correct. A more efficient algorithm would be to
        # check the nodes that were added to see if they should be in the
        # fringe, or if they have potentially affected the status of Tasks that
        # were done (error case - disallowed??)

        if all_failed:
            logger.info("DAG execute finished, failed {}".format(all_failed))
            return False, len(all_completed), len(already_done), len(all_failed)
        else:
            logger.info("DAG execute finished successfully, {} jobs"
                        .format(len(all_completed)))
            return True, len(all_completed), len(already_done), len(all_failed)

    def sort_jobs(self, runners, completed_and_failed):
        """
        Sort into two list of completed and failed, and return an update
        runners dict
        TBD don't like the side effect on runners

        Args:
            runners (dictionary): of currently running jobs, by job_id
            completed_and_failed (list): List of tuples of (job_id, JobStatus)

        Returns:
            A new runners dictionary, two lists of job_ids
        """
        completed = []
        failed = []
        for (jid, status) in completed_and_failed:
            if jid in runners:
                task = runners.pop(jid)
            task.status = status

            if status == JobStatus.DONE:
                completed += [task]
            elif status == JobStatus.ERROR_FATAL:
                failed += [task]
            else:
                raise ValueError("Job returned that is neither done nor "
                                 "error_fatal: jid: {}, status {}"
                                 .format(jid, status))
        return runners, completed, failed

    def propagate_results(self, task):
        """
        For all its downstream tasks, is that task now ready to run?
        Also marks the Task as DONE

        Args:
            task: The task that just completed
        Returns:
            Tasks to be added to the fringe
        """
        new_fringe = []

        logger.debug("Propagate {}".format(task))
        task.set_status(JobStatus.DONE)
        for downstream in task.downstream_tasks:
            logger.debug("  downstream {}".format(downstream))
            downstream_done = (downstream.status == JobStatus.DONE)
            if downstream.all_upstreams_done() and not downstream_done:
                logger.debug("  and add to fringe")
                new_fringe += [downstream]
                # else Nothing - that Task ain't ready yet
            else:
                logger.debug("  not ready yet")
        return new_fringe

    def _find_task(self, task_hash):
        """
        Args:
           hash:

        Return:
            The Task with that hash
        """
        return self.tasks[task_hash]

    def add_task(self, task):
        """
        Set semantics - add tasks once only, based on hash name.
        Also creates the job. If is_new has no task_id then
        creates task_id and writes it onto object.

        Returns:
           The Job

        Raises:
            ValueError if a task is trying to be added but it already exists
        """
        logger.debug("Adding Task {}".format(task))
        if task.hash in self.tasks:
            raise ValueError("A task with hash '{}' already exists"
                             .format(task.hash))
        self.tasks[task.hash] = task
        logger.debug("Task {} added".format(task.hash))
        return task

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    def _set_top_fringe(self):
        self.top_fringe = []
        for task in self.tasks.values():
            unfinished_upstreams = [u for u in task.upstream_tasks
                                    if u.status != JobStatus.DONE]
            if not unfinished_upstreams and task.status != JobStatus.DONE:
                self.top_fringe += [task]
        return self.top_fringe

    def __repr__(self):
        """
        Very useful for debug logs, but this won't scale!
        Returns:
            String useful for debugging logs
        """
        s = "[DAG id={id}: '{n}':\n".format(id=self.dag_id, n=self.name)
        for t in self.tasks.values():
            s += "  {}\n".format(t)
        s += "]"
        return s

    @property
    def hash(self):
        hashval = hashlib.sha1()
        for task_hash in sorted(self.tasks):
            hashval.update(bytes("{:x}".format(task_hash).encode('utf-8')))
            task = self.tasks[task_hash]
            for dtask in sorted(task.downstream_tasks):
                hashval.update(bytes("{:x}".format(dtask.hash).encode('utf-8')))
        return hashval.hexdigest()
