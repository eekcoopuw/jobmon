import logging

from sqlalchemy import Column, DateTime, Integer, String

from jobmon.models import JobStatus
from jobmon.sql_base import Base

logger = logging.getLogger(__name__)


class TaskDag(Base):
    """
    A DAG of Tasks.
    """
    __tablename__ = 'task_dag'

    dag_id = Column(Integer, primary_key=True)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime)

    def __init__(self, dag_id=None, name=None, user=None,
                 job_list_manager=None, created_date=None):
        # TBD input validation, specifically dag_id == None
        super(TaskDag, self).__init__(dag_id=dag_id, name=name, user=user,
                                      created_date=created_date)
        self.job_list_manager = job_list_manager

        # dictionary, TBD needs to scale to 1,000,000 jobs, untested at scale
        self.names_to_nodes = {}
        self.top_fringe = []
        self.fail_after_n_executions = None

    def _set_fail_after_n_executions(self, n):
        """
        For use during testing, force the TaskDag to 'fall over' after n
        executions, so that the resume case can be tested.

        In every non-test case, self.fail_after_n_executions will be None, and
        thus will never equal the number of executions and so the ValueError
        'fall over' will not be triggered in production. """
        self.fail_after_n_executions = n

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
        if len(self.names_to_nodes):
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

        logger.debug("self.fail_after_n_executions is {}"
                     .format(self.fail_after_n_executions))
        fringe = self.top_fringe

        all_completed = []
        all_failed = []
        all_running = {}
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
                if not task.am_i_done():
                    logger.debug("Queueing newly ready task {}".format(task))
                    task.queue_job(self.job_list_manager)
                    all_running[task.job_id] = task

            # TBD timeout?
            completed_and_status = (
                self.job_list_manager.block_until_any_done_or_error())
            for job in completed_and_status:
                if job[1] == JobStatus.DONE:
                    n_executions += 1
            logger.debug("Return from blocking call, completed_and_status {}"
                         .format(completed_and_status))
            all_running, completed_tasks, failed_tasks = self.sort_jobs(
                all_running, completed_and_status)

            # Need to find the tasks that were that job, they will be in this
            # "small" dic of active tasks

            all_completed += completed_tasks
            all_failed += failed_tasks
            for task in completed_tasks:
                fringe += self.propagate_results(task)
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
            return False, len(all_completed), len(all_failed)
        else:
            logger.info("DAG execute finished successfully, {} jobs"
                        .format(len(all_completed)))
            return True, len(all_completed), len(all_failed)

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
            task = runners.pop(jid)
            task.cached_status = status

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
            if not downstream.am_i_done() and downstream.all_upstreams_done():
                logger.debug("  and add to fringe")
                new_fringe += [downstream]
                # else Nothing - that Task ain't ready yet
            else:
                logger.debug("  not ready yet")
        return new_fringe

    def _find_task(self, hash_name):
        """
        Args:
           hash_name:

        Return:
            The Task with that hash_name
        """
        return self.names_to_nodes[hash_name]

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
        if task.hash_name in self.names_to_nodes:
            raise ValueError("A task with hash_name '{}' already exists"
                             .format(task.hash_name))
        self.names_to_nodes[task.hash_name] = task
        if not task.upstream_tasks:
            self.top_fringe += [task]

        logger.debug("Creating Job {}".format(task.hash_name))
        job = task.create_job(self.job_list_manager)
        return job

    def __repr__(self):
        """
        Very useful for devug logs, but this won't scale!
        Returns:
            String useful for debugging logs
        """
        s = "[DAG id={id}: '{n}':\n".format(id=self.dag_id, n=self.name)
        for t in self.names_to_nodes.values():
            s += "  {}\n".format(t)
        s += "]"
        return s
