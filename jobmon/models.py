import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from datetime import datetime


Base = declarative_base()


class InvalidStateTransition(Exception):
    def __init__(self, model, old_state, new_state):
        msg = "Cannot transition {} from {} to {}".format(
            model, old_state, new_state)
        super().__init__(self, msg)


class JobStatus(Base):
    __tablename__ = 'job_status'

    REGISTERED = 1
    QUEUED_FOR_INSTANTIATION = 2
    INSTANTIATED = 3
    RUNNING = 4
    ERROR_RECOVERABLE = 5
    ERROR_FATAL = 6
    DONE = 7

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class JobInstanceStatus(Base):
    __tablename__ = 'job_instance_status'

    INSTANTIATED = 1
    SUBMITTED_TO_BATCH_EXECUTOR = 2
    RUNNING = 3
    ERROR = 4
    DONE = 5

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class Job(Base):
    __tablename__ = 'job'

    @classmethod
    def from_wire(cls, dct):
        return cls(dag_id=dct['dag_id'], job_id=dct['job_id'],
                   name=dct['name'], command=dct['command'],
                   status=dct['status'], num_attempts=dct['num_attempts'],
                   max_attempts=dct['max_attempts'])

    def to_wire(self):
        return {'dag_id': self.dag_id, 'job_id': self.job_id, 'name':
                self.name, 'command': self.command, 'status': self.status,
                'num_attempts': self.num_attempts,
                'max_attempts': self.max_attempts}

    job_id = Column(Integer, primary_key=True)
    job_instances = relationship("JobInstance", back_populates="job")
    dag_id = Column(
        Integer,
        ForeignKey('job_dag.dag_id'))
    name = Column(String(150))
    command = Column(String(1000))
    num_attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=1)
    max_runtime = Column(Integer)
    status = Column(
        Integer,
        ForeignKey('job_status.id'),
        nullable=False)
    submitted_date = Column(DateTime, default=datetime.utcnow())
    status_date = Column(DateTime, default=datetime.utcnow())

    valid_transitions = [
        (JobStatus.REGISTERED, JobStatus.QUEUED_FOR_INSTANTIATION),
        (JobStatus.QUEUED_FOR_INSTANTIATION, JobStatus.INSTANTIATED),
        (JobStatus.INSTANTIATED, JobStatus.RUNNING),
        (JobStatus.INSTANTIATED, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.RUNNING, JobStatus.DONE),
        (JobStatus.RUNNING, JobStatus.ERROR_RECOVERABLE),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.ERROR_FATAL),
        (JobStatus.ERROR_RECOVERABLE, JobStatus.QUEUED_FOR_INSTANTIATION)]

    def transition_to_error(self):
        self.transition(JobStatus.ERROR_RECOVERABLE)
        if self.num_attempts >= self.max_attempts:
            self.transition(JobStatus.ERROR_FATAL)
        else:
            self.transition(JobStatus.QUEUED_FOR_INSTANTIATION)

    def transition(self, new_state):
        self._validate_transition(new_state)
        if new_state == JobStatus.INSTANTIATED:
            self.num_attempts = self.num_attempts + 1
        self.status = new_state
        self.status_date = datetime.utcnow()

    def _validate_transition(self, new_state):
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('Job', self.status, new_state)


class JobInstance(Base):
    __tablename__ = 'job_instance'

    @classmethod
    def from_wire(cls, dct):
        return cls(job_instance_id=dct['job_instance_id'],
                   executor_id=dct['executor_id'],
                   job_id=dct['job_id'],
                   status=dct['status'],
                   status_date=datetime.strptime(dct['status_date'],
                                                 "%Y-%m-%dT%H:%M:%S"))

    def to_wire(self):
        time_since_status = (datetime.utcnow()-self.status_date).seconds
        return {'job_instance_id': self.job_instance_id,
                'executor_id': self.executor_id,
                'job_id': self.job_id,
                'status': self.status,
                'status_date': self.status_date.strftime("%Y-%m-%dT%H:%M:%S"),
                'time_since_status_update': time_since_status,
                'max_runtime': self.job.max_runtime}

    job_instance_id = Column(Integer, primary_key=True)
    executor_type = Column(String(50))
    executor_id = Column(Integer)
    job_id = Column(
        Integer,
        ForeignKey('job.job_id'),
        nullable=False)
    job = relationship("Job", back_populates="job_instances")
    usage_str = Column(String(250))
    wallclock = Column(String(50))
    maxvmem = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    status = Column(
        Integer,
        ForeignKey('job_instance_status.id'),
        default=JobInstanceStatus.INSTANTIATED,
        nullable=False)
    submitted_date = Column(DateTime, default=datetime.utcnow())
    status_date = Column(DateTime, default=datetime.utcnow())

    valid_transitions = [
        (JobInstanceStatus.INSTANTIATED, JobInstanceStatus.RUNNING),

        (JobInstanceStatus.INSTANTIATED,
         JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.RUNNING),

        (JobInstanceStatus.SUBMITTED_TO_BATCH_EXECUTOR,
         JobInstanceStatus.ERROR),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.ERROR),

        (JobInstanceStatus.RUNNING, JobInstanceStatus.DONE)]

    def transition(self, new_state):
        self._validate_transition(new_state)
        self.status = new_state
        self.status_date = datetime.utcnow()
        if new_state == JobInstanceStatus.RUNNING:
            self.job.transition(JobStatus.RUNNING)
        elif new_state == JobInstanceStatus.DONE:
            self.job.transition(JobStatus.DONE)
        elif new_state == JobInstanceStatus.ERROR:
            self.job.transition_to_error()

    def _validate_transition(self, new_state):
        if (self.status, new_state) not in self.__class__.valid_transitions:
            raise InvalidStateTransition('JobInstance', self.status, new_state)


class JobInstanceErrorLog(Base):
    __tablename__ = 'job_instance_error_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = Column(DateTime, default=datetime.utcnow())
    description = Column(String(1000), nullable=False)


class JobInstanceStatusLog(Base):
    __tablename__ = 'job_instance_status_log'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    status = Column(
        Integer,
        ForeignKey('job_instance_status.id'),
        nullable=False)
    status_time = Column(DateTime, default=datetime.utcnow())

logger = logging.getLogger(__name__)
class JobDag(Base):
    """
    A DAG of Tasks.
    In the final implementation it will probably use networkX under the hood, but for the moment it is not needed.
    Still not needed. YAGNI
    """

    __tablename__ = 'job_dag'

    dag_id = Column(Integer, primary_key=True)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime, default=func.now())

    def __init__(self, dag_id=None, name=None, user=None, job_list_manager=None, job_state_manager=None, batch_id=None):
        # TBD input validation.
        # TBD Handling of dag_id == None
        self.dag_id = dag_id
        self.name = name
        self.user = user
        self.job_list_manager = job_list_manager
        self.job_state_manager = job_state_manager

        # A DiGraph is a Directed Simple graph, it allows loops.
        # self.task_graph = nx.DiGraphph()
        self.names_to_nodes = {}  # dictionary, TBD needs to scale to 1,000,000 jobs and have set semantics
        self.batch_id = batch_id
        self.graph_built = False
        self.top_fringe = []

    def get_node_from_name(self, name):
        """
        Behind an interface in case it is not a simple dictionary in future
        :param name:
        :return:
        """
        return self.names_to_nodes[name]

    def validate(self, raises=True):
        """
        Mostly useful for testing, but also for debugging, and also as a solid example of how this complex data
        structure should look.

        Check:
          - Only one graph - not two sub graphs
          - Ask each task to check its upstreams against what it thinks they should be
          - External Tasks cannot have upstreams
          - TBD

        :return: returns True if no problems. if raises is True, then it raises ValueError on first problem, else False
        """
        # TBD Finish implementing this
        self._build_graph()

        # The empty graph cannot have errors
        if len(self.names_to_nodes):
            return True

        error_message = None

        # TBD Is it worth keeping NetrowkX just to check itis a DAG?
        # if not nx.is_directed_acyclic_graph(self.task_graph):
        #     error_message = "The graph is not a DAG"
        if not self.top_fringe:
            error_message = "The graph has no top-fringe, an algorithm bug."

        if error_message:
            if raises:
                raise ValueError(error_message)  # TBD custom exception type?
            else:
                logger.warning(error_message)
                return False
        return True

    def execute(self):
        """
        Take a concrete DAG and create_and_queue_job all the Tasks that are not done and either have never run, or who are out-of-date
        with respect to their inputs.

        Uses forward chaining from initial fringe, hence out-of-date is not applied transitively backwards through the graph.
        It could also use backward chaining from an identified goal node, the effect is identical.

        TBD: The internal data structures are lists, but will need to be changed to be better at scaling.
        TBD No database backing
        :return:
        """

        # Conceptually:
        # Mark all Tasks as not_done for this execution
        # while the fringe is not empty:
        #   if the job is not out of date, skip it and add its downstreams to the fringe
        #   if not, queue it
        #   wait for some jobs to complete
        #   rinse and repeat
        # wait for the last jobs to complete

        # A task is 'done' for this dag execution when it is skipped, or it has successfully run
        self._build_graph()
        self._mark_done()

        fringe = self.top_fringe

        all_completed = []
        all_skipped = []
        all_failed = []

        # These are all nxgraph nodes, each of which is-a Task
        while fringe:
            # Everything in the fringe should be run or skipped
            to_queue = []
            to_skip = []
            for task in fringe:
                if task.needs_to_execute():
                    to_queue += task
                else:
                    to_skip += task
                    # The job is eligible to run, but its outputs are not out-of-date wrto inputs.
                    # Therefore we can skip it.

            fringe = []
            # Start all those new jobs ASAP
            for task in to_queue:
                task.queue_job(self.job_list_manager)
                # self.job_list_manager.queue_job(task.job.job_id, jobmon_params)
                # job_queue.queue_job then calls Task.create_and_queue_job( self, jobmon_params)

            # And propagate the results for the skipped ones
            if to_skip:
                all_skipped += to_skip
                for task in to_skip:
                    fringe += self.propagate_results(task)
            else:
                # nothing was skipped, therefore need to wait for something to be done
                completed, failed = self.job_list_manager.block_until_any_done_or_error(poll_interval=60)
                all_completed += completed
                all_failed += failed
                for task in completed:
                    fringe += self.propagate_results(task)

        # end while fringe

        # TBD - must wait if any are still queued
        self.job_list_manager.block_until_no_instances(poll_interval=60)

        # To be a type-3 tool, we must be prepared for the DAG to have changed
        # In general we would recompute forward from the the fringe. Not efficient, but correct.
        # A more efficient algorithm would be to check the nodes that were added to see if they should be in the fringe,
        # or if they have potentially affected the status of Tasks that were done (error case - disallowed??)

        if all_failed:
            return False
        else:
            return True

    def propagate_results(self, task):
        """
        :param task:
        :return: tasks to be added to the fringe
        """
        new_fringe = []

        # For all its downstream tasks, is that task now ready to run?
        task.done = True
        for downstream in task.downstream_tasks:  # TBD networkX?
            if not downstream.done and downstream.all_upstreams_done():
                new_fringe += downstream
                # else Nothing - that Task ain't ready yet
        return new_fringe

    def find_task(self, logical_name):
        """
        :param logical_name:
        :return:
        """
        return self.names_to_nodes[logical_name]

    def add_task(self, task):
        """
        Set semantics - add tasks once only, based on logical name.
        Also creates the job. If is_new has no task_id then
        creates task_id and writes it onto object.

        is_new for safety.

        TBD Creating task_id - go to the database?

        :return: The task, this is same process
        """
        # TBD validation of inputs
        logger.info("Adding Task {}".format(task))
        self.names_to_nodes[task.logical_name] = task
        if not task.upstream_tasks:
            self.top_fringe += [task]

        logger.info("Creating Job {}".format(task.logical_name))
        job_id = task.create_job(self.job_list_manager)
        return job_id

    def _mark_done(self):
        """
        Mark them all not-done.
        Connect up their names into a graph.
        Create the jobs, but do not queue them
        :return:
        """
        for task in self.names_to_nodes.values():
            task.done = False

    def _build_graph(self):
        """
        Ensure that it is all loaded from the database into memory.

        TBD Switch to a cached algorithm for finding top_fringe by dbs query or during build.
        Would require private access for Upstream_tasks with a setter.
        :return:
        """

        all_jobs_by_job_id = self.job_state_manager.get_jobs(self.dag_id)
        for task in self.names_to_nodes.values():
            task.load(all_jobs_by_job_id)
        return
        # if not self.graph_built:
        #     self.top_fringe = []
        #     self.graph_built = True
        #     for task in self.names_to_nodes.values():
        #         if not task.upstream_tasks:
        #             self.top_fringe += [task]
        #         # self.operator_graph.add_edges(upstreams)
        #         task.create_job(self.job_list_manager)
