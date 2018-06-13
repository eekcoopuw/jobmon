import getpass
import hashlib
import logging
from datetime import datetime
import uuid

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from jobmon.config import config
from jobmon.exceptions import ReturnCodes
from jobmon.requester import Requester
from jobmon.sql_base import Base
from jobmon.workflow.workflow_run import WorkflowRun
from jobmon.workflow.task_dag import TaskDag

logger = logging.getLogger(__name__)


class WorkflowAlreadyComplete(Exception):
    pass


class WorkflowStatus(Base):
    __tablename__ = 'workflow_status'

    CREATED = 'C'
    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = Column(String(1), primary_key=True)
    label = Column(String(150), nullable=False)


class WorkflowDAO(Base):

    __tablename__ = 'workflow'

    @classmethod
    def from_wire(cls, dct):
        return cls(id=dct['id'], dag_id=dct['dag_id'],
                   workflow_args=dct['workflow_args'],
                   workflow_hash=dct['workflow_hash'],
                   description=dct['description'], name=dct['name'],
                   user=dct['user'], status=dct['status'])

    def to_wire(self):
        return {'id': self.id, 'dag_id': self.dag_id, 'workflow_args':
                self.workflow_args, 'workflow_hash': self.workflow_hash,
                'description': self.description, 'name': self.name, 'user':
                self.user, 'status': self.status}

    id = Column(Integer, primary_key=True)
    dag_id = Column(Integer, ForeignKey('task_dag.dag_id'))
    workflow_args = Column(Text)
    workflow_hash = Column(Text)
    description = Column(Text)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime, default=datetime.utcnow)
    status_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String(1),
                    ForeignKey('workflow_status.id'),
                    nullable=False,
                    default=WorkflowStatus.CREATED)

    workflow_runs = relationship("WorkflowRunDAO", back_populates="workflow")
    task_dag = relationship("TaskDagMeta", back_populates="workflow")


class Workflow(object):
    """(aka Batch, aka Swarm)
    A Workflow is a framework by which a user may define the relationship
    between tasks and define the relationship between multiple runs of the same
    set of tasks. The great benefit of the Workflow is that it's resumable.
    A Workflow can only be re-loaded if two things are shown to be exact
    matches to a previous Workflow:

    1. WorkflowArgs: It is recommended to pass a meaningful unique identifier
        to workflow_args, to ease resuming. However, if the Workflow is a
        one-off project, you may instantiate the Workflow anonymously, without
        WorkflowArgs. Under the hood, the WorkflowArgs will default to a UUID
        which, as it is randomly generated, will be harder to remember and thus
        harder to resume.

        Workflow args must be hashable. For example, CodCorrect or Como version
        might be passed as Args to the Workflow. For now, the assumption is
        WorkflowArgs is a string.

    2. The tasks added to the workflow. A Workflow's TaskDag is built up by
        using Workflow.add_task(). In order to resume a Workflow, all the same
        tasks must be added with the same dependencies between tasks.
    """

    def __init__(self, task_dag=None, workflow_args=None, name="",
                 description="", stderr=None, stdout=None, project=None):
        self.wf_dao = None
        self.name = name
        self.description = description

        self.stderr = stderr
        self.stdout = stdout
        self.project = project

        self.jsm_req = Requester(config.jm_rep_conn)
        self.jqs_req = Requester(config.jqs_rep_conn)

        if task_dag:
            self.task_dag = task_dag
        else:
            self.task_dag = TaskDag()

        if workflow_args:
            self.workflow_args = workflow_args
        else:
            self.workflow_args = str(uuid.uuid4())
            logger.info("Workflow_args defaulting to uuid {}. To resume this "
                        "workflow, you must re-instantiate Workflow and pass "
                        "this uuid in as the workflow_args. As a uuid is hard "
                        "to remember, we recommend you name your workflows and"
                        " make workflow_args a meaningful unique identifier. "
                        "Then add the same tasks to this workflow"
                        .format(self.workflow_args))

    @property
    def dag_id(self):
        if self.is_bound:
            return self.wf_dao.dag_id
        else:
            raise AttributeError("Workflow is not yet bound")

    @property
    def hash(self):
        if self.is_bound:
            return self.wf_dao.workflow_hash
        else:
            raise AttributeError("Workflow is not yet bound")

    @property
    def id(self):
        if self.is_bound:
            return self.wf_dao.id
        else:
            raise AttributeError("Workflow is not yet bound")

    @property
    def is_bound(self):
        if self.wf_dao:
            return True
        else:
            return False

    @property
    def status(self):
        if self.is_bound:
            return self.wf_dao.status
        else:
            raise AttributeError("Workflow is not yet bound")

    def add_task(self, task):
        return self.task_dag.add_task(task)

    def add_tasks(self, tasks):
        self.task_dag.add_tasks(tasks)

    def _bind(self):
        potential_wfs = self._matching_workflows()
        if len(potential_wfs) == 1:

            # TODO: Should prompt the user, asking whether they really want to
            # resume or whether they want to create a new Workflow... will need
            # to sit outside this if/else block
            self.wf_dao = potential_wfs[0]
            if self.wf_dao.status == WorkflowStatus.DONE:
                raise WorkflowAlreadyComplete
            self.task_dag.bind_to_db(
                self.dag_id,
                executor_args={'stderr': self.stderr,
                               'stdout': self.stdout,
                               'project': self.project, })
        elif len(potential_wfs) == 0:
            # Bind the dag ...
            self.task_dag.bind_to_db(executor_args={'stderr': self.stderr,
                                                    'stdout': self.stdout,
                                                    'project': self.project, })

            # Create new workflow in Database
            rc, wf_dct = self.jsm_req.send_request({
                'action': 'add_workflow',
                'kwargs': {'dag_id': self.task_dag.dag_id,
                           'workflow_args': self.workflow_args,
                           'workflow_hash': self._compute_hash(),
                           'name': self.name,
                           'description': self.description,
                           'user': getpass.getuser()}
            })
            self.wf_dao = WorkflowDAO.from_wire(wf_dct)
        else:
            # This case should never happen... we have application side
            # protection against this, but we should probably force the
            # validation down into the DB layer as well (i.e. make the
            # dag_hash + workflow_args a unique tuple)
            # TODO: Protect against duplicated dag+workflow_args at DB level
            raise RuntimeError("Multiple matching Workflows found {}. "
                               "Workflows should be unique on TaskDag and "
                               "WorkflowArgs".format(potential_wfs))

    def _done(self):
        self.workflow_run.update_done()
        self._update_status(WorkflowStatus.DONE)

    def _compute_hash(self):
        hashval = hashlib.sha1()
        hashval.update(bytes(self.workflow_args.encode('utf-8')))
        hashval.update(bytes(self.task_dag.hash.encode('utf-8')))
        return hashval.hexdigest()

    def _create_workflow_run(self):
        # Create new workflow in Database
        self.workflow_run = WorkflowRun(self.id, self.stderr, self.stdout,
                                        self.project)

    def _error(self):
        self.workflow_run.update_error()
        self._update_status(WorkflowStatus.ERROR)

    def _matching_dag_ids(self):
        rc, dag_ids = self.jqs_req.send_request({
            'action': 'get_dag_ids_by_hash',
            'kwargs': {'dag_hash': self.task_dag.hash}
        })
        return dag_ids

    def _matching_workflows(self):
        dag_ids = self._matching_dag_ids()
        workflows = []
        for dag_id in dag_ids:
            rc, wf = self.jqs_req.send_request({
                'action': 'get_workflows_by_inputs',
                'kwargs': {'dag_id': dag_id,
                           'workflow_args': self.workflow_args}
            })
            if rc == ReturnCodes.OK:
                workflows.append(wf)
        return [WorkflowDAO.from_wire(w) for w in workflows]

    def _update_status(self, status):
        rc, wf_dct = self.jsm_req.send_request({
            'action': 'update_workflow',
            'kwargs': {'wf_id': self.id, 'status': status}
        })
        self.wf_dao = WorkflowDAO.from_wire(wf_dct)

    def execute(self):
        if not self.is_bound:
            self._bind()
        self._create_workflow_run()
        success, n_new_done, n_prev_done, n_failed = (
            self.task_dag._execute_interruptible())

        if success:
            self._done()
        else:
            self._error()
        self.report(success, n_new_done, n_prev_done, n_failed)
        return success

    def report(self, success, n_new_done, n_prev_done, n_failed):
        if success:
            logger.info("Workflow finished successfully!")
            logger.info("# finished jobs: {}".format(n_new_done + n_prev_done))
        else:
            logger.info("Workflow FAILED")
            logger.info("# finished jobs (this run): {}".format(n_new_done))
            logger.info("# finished jobs (previous runs): {}"
                        .format(n_prev_done))
            logger.info("# failed jobs: {}".format(n_failed))

    def run(self):
        """Alias for self.execute"""
        return self.execute()
