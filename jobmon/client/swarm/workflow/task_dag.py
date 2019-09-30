import getpass
import hashlib
import copy
from collections import OrderedDict

from jobmon.client.swarm.job_management.job_list_manager import JobListManager
from jobmon.models.job_status import JobStatus
from jobmon.client.swarm.workflow.task_dag_factory import TaskDagMetaFactory
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class DagExecutionStatus(object):
    """Enumerate possible exit statuses for TaskDag._execute()"""

    SUCCEEDED = 0
    FAILED = 1
    STOPPED_BY_USER = 2


class TaskDag(object):
    """A DAG of ExecutableTasks."""

    def __init__(self, name="", executor=None, fail_fast=False,
                 job_instantiation_interval=10, seconds_until_timeout=36000):
        """
        Args:
            name (str): name of dag
            executor (Executor): executor instance being used
            fail_fast (bool): whether to fail after a task fails
            job_instantiation_interval (int): number of seconds to wait before
                instantiating newly ready jobs
            seconds_until_timeout (int): length that the workflow itself will
                run before timing out

        """

        self.dag_id = None
        self.job_list_manager = None

        self.name = name

        self.tasks = OrderedDict()  # {job_hash: ExecutableTask}
        self.bound_tasks = None
        self.top_fringe = []
        self.fail_after_n_executions = None

        self.tags = set()

        self.fail_fast = fail_fast

        self.executor = executor
        self.job_instantiation_interval = job_instantiation_interval
        self.seconds_until_timeout = seconds_until_timeout

    def _set_fail_after_n_executions(self, n):
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        so the 'fall over' will not be triggered in production.
        """
        self.fail_after_n_executions = n

    @property
    def is_bound(self):
        """Boolean indicating whether the Dag is bound to the DB"""
        if self.dag_id:
            return True
        else:
            return False

    def bind_to_db(self, dag_id=None, reset_running_jobs=True):
        """
        Binds the dag to the database and starts Job Management services.
        The Job_List_Manger is stopped and discarded in _clean_up_after_run

        Args:
            dag_id (int): Defaults to None, in which case a new dag_id is
                created
            reset_running_jobs (bool) : Set this to true in the "cold resume"
                use case, where the human operator knows that there are no
                jobs running, in which case reset their statuses in the
                database
        """
        if dag_id:
            self.job_list_manager = JobListManager(
                dag_id, executor=self.executor, start_daemons=True,
                job_instantiation_interval=self.job_instantiation_interval)

            for _, task in self.tasks.items():
                self.job_list_manager.bind_task(task)

            # We only want to reset jobs in the cold-resume case. In
            # the hot-resume case, we want to allow running jobs to continue
            # and only pickup failed / not-yet-run jobs
            #
            # Reset any jobs hung up in not-DONE states
            if reset_running_jobs:
                self.job_list_manager.reset_jobs()

            self.dag_id = dag_id
        else:
            tdf = TaskDagMetaFactory()
            self.meta = tdf.create_task_dag(name=self.name, dag_hash=self.hash,
                                            user=getpass.getuser())
            self.job_list_manager = JobListManager(
                self.meta.dag_id, executor=self.executor, start_daemons=True)
            self.dag_id = self.meta.dag_id

            # Bind all the tasks to the job_list_manager
            for _, task in self.tasks.items():
                self.job_list_manager.bind_task(task)
        self.bound_tasks = self.job_list_manager.bound_tasks

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

    def disconnect(self):
        if self.job_list_manager:
            self.job_list_manager.disconnect()

    def _execute(self):
        """
        Take a concrete DAG and queue all the Tasks that are not DONE.

        Uses forward chaining from initial fringe, hence out-of-date is not
        applied transitively backwards through the graph. It could also use
        backward chaining from an identified goal node, the effect is
        identical.

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
        self.job_list_manager.log_dag_running()

        previously_completed = copy.copy(self.job_list_manager.all_done)
        self._set_top_fringe()

        logger.debug("self.fail_after_n_executions is {}"
                     .format(self.fail_after_n_executions))
        fringe = copy.copy(self.top_fringe)

        n_executions = 0

        logger.debug("Execute DAG {}".format(self))
        # These are all Tasks.
        # While there is something ready to be run, or something is running
        while fringe or self.job_list_manager.active_jobs:
            # Everything in the fringe should be run or skipped,
            # they either have no upstreams, or all upstreams are marked DONE
            # in this execution

            while fringe:
                # Get the front of the queue, add to the end.
                # That ensures breadth-first behavior, which is likely to
                # maximize parallelism
                task = fringe.pop(0)
                # Start the new jobs ASAP
                if task.is_done:
                    raise RuntimeError("Invalid DAG. Encountered a DONE node.")
                else:
                    logger.debug("Instantiating resources for newly ready "
                                 "task and changing it to the queued state {}"
                                 .format(task))
                    self.job_list_manager.adjust_resources_and_queue(task)

            # TBD timeout?
            # An exception is raised if the runtime exceeds the timeout limit
            completed_tasks, failed_tasks = (
                self.job_list_manager.block_until_any_done_or_error(
                    timeout=self.seconds_until_timeout))
            for task in completed_tasks:
                n_executions += 1
            if failed_tasks and self.fail_fast is True:
                break  # fail out early
            logger.debug(
                "Return from blocking call, completed: {}, failed: {}".format(
                    [t.job_id for t in completed_tasks],
                    [t.job_id for t in failed_tasks]))

            for task in completed_tasks:
                task_to_add = self.propagate_results(task)
                fringe = list(set(fringe + task_to_add))
            if (self.fail_after_n_executions is not None and
                    n_executions >= self.fail_after_n_executions):
                self.job_list_manager.disconnect()
                raise ValueError("Dag asked to fail after {} executions. "
                                 "Failing now".format(n_executions))

        # END while fringe or all_running

        # To be a dynamic-DAG  tool, we must be prepared for the DAG to have
        # changed. In general we would recompute forward from the the fringe.
        # Not efficient, but correct. A more efficient algorithm would be to
        # check the nodes that were added to see if they should be in the
        # fringe, or if they have potentially affected the status of Tasks that
        # were done (error case - disallowed??)

        all_completed = self.job_list_manager.all_done
        num_new_completed = len(all_completed) - len(previously_completed)
        all_failed = self.job_list_manager.all_error
        if all_failed:
            if self.fail_fast:
                logger.info("Failing after first failure, as requested")
            logger.info("DAG execute finished, failed {}".format(all_failed))
            self.job_list_manager.disconnect()
            return (DagExecutionStatus.FAILED, num_new_completed,
                    len(previously_completed), len(all_failed))
        else:
            logger.info("DAG execute finished successfully, {} jobs"
                        .format(num_new_completed))
            self.job_list_manager.disconnect()
            return (DagExecutionStatus.SUCCEEDED, num_new_completed,
                    len(previously_completed), len(all_failed))

    def _execute_interruptible(self):
        keep_running = True
        while keep_running:
            try:
                return self._execute()
            except KeyboardInterrupt:
                confirm = input("Are you sure you want to exit (y/n): ")
                confirm = confirm.lower().strip()
                if confirm == "y":
                    keep_running = False
                    self.disconnect()
                    return (DagExecutionStatus.STOPPED_BY_USER,
                            len(self.job_list_manager.all_done), None, None)
                else:
                    print("Continuing jobmon execution...")
            finally:
                # In a finally block so clean up always occurs
                self._clean_up_after_run()

    def _clean_up_after_run(self) -> None:
        """
        Make sure all the threads are stopped. The JLM is created in bind_db
        """
        self.job_list_manager.disconnect()

    def reconnect(self) -> None:
        if self.job_list_manager:
            self.job_list_manager.connect()

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
        for downstream in task.downstream_tasks:
            logger.debug("  downstream {}".format(downstream))
            downstream_done = (downstream.status == JobStatus.DONE)
            if (not downstream_done and downstream.status ==
                JobStatus.REGISTERED):
                if downstream.all_upstreams_done:
                    logger.debug("  and add to fringe")
                    new_fringe += [downstream]  # make sure there's no dups
                    # else Nothing - that Task ain't ready yet
                else:
                    logger.debug("  not ready yet")
            else:
                logger.debug("  not ready yet or already queued. Status is {}"
                             .format(downstream.status))
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
            raise ValueError("A task with hash '{}' already exists. All tasks "
                             "in a workflow must have unique commands"
                             .format(task.hash))
        self.tasks[task.hash] = task
        self.tags.add(task.tag)
        logger.debug("Task {} added".format(task.hash))
        return task

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    def _set_top_fringe(self):
        self.top_fringe = []
        for task in self.bound_tasks.values():
            unfinished_upstreams = [u for u in task.upstream_tasks
                                    if u.status != JobStatus.DONE]
            if not unfinished_upstreams and \
                    task.status == JobStatus.REGISTERED:
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
                hashval.update(
                    bytes("{:x}".format(dtask.hash).encode('utf-8')))
        return hashval.hexdigest()
