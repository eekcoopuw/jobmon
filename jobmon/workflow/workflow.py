import getpass
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from jobmon.config import config
from jobmon.requester import Requester
from jobmon.sql_base import Base
from jobmon.workflow.workflow_run import WorkflowRun


class WorkflowStatus(Base):
    __tablename__ = 'workflow_status'

    CREATED = 1
    RUNNING = 2
    STOPPED = 3
    COMPLETE = 4

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class WorkflowDAO(Base):

    __tablename__ = 'workflow'

    @classmethod
    def from_wire(cls, dct):
        return cls(dag_id=dct['dag_id'], job_id=dct['job_id'],
                   name=dct['name'], command=dct['command'],
                   slots=dct['slots'], mem_free=dct['mem_free'],
                   project=dct['project'], status=dct['status'],
                   num_attempts=dct['num_attempts'],
                   max_attempts=dct['max_attempts'],
                   context_args=dct['context_args'])

    def to_wire(self):
        return {'dag_id': self.dag_id, 'job_id': self.job_id, 'name':
                self.name, 'command': self.command, 'status': self.status,
                'slots': self.slots, 'mem_free': self.mem_free,
                'project': self.project, 'num_attempts': self.num_attempts,
                'max_attempts': self.max_attempts,
                'context_args': self.context_args}

    id = Column(Integer, primary_key=True)
    dag_id = Column(Integer, ForeignKey('task_dag.dag_id'))
    workflow_args = Column(Text)
    description = Column(Text)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime, default=datetime.utcnow())
    status_date = Column(DateTime, default=datetime.utcnow())
    status = Column(Integer,
                    ForeignKey('workflow_status.id'),
                    nullable=False,
                    default=WorkflowStatus.CREATED)

    workflow_runs = relationship("WorkflowRunDAO", back_populates="workflow")


class Workflow(object):

    def __init__(self, task_dag, workflow_args, name="", description=""):
        self.id = None
        self.name = name
        self.description = description
        self.task_dag = task_dag
        self.workflow_args = workflow_args

        self.jsm_req = Requester(config.jm_rep_conn)
        self.jqs_req = Requester(config.jqs_rep_conn)

    @property
    def bound(self):
        if self.id:
            return True
        else:
            return False

    def _matching_dag_ids(self):
        rc, dag_ids = self.jqs_req.send_request({
            'action': 'get_dag_ids_by_hash',
            'kwargs': {'dag_hash': self.task_dag.hash}
        })
        return dag_ids

    def _matching_workflows(self):
        dag_ids = self._matching_dag_ids()
        wf_dag = {}
        for dag_id in dag_ids:
            rc, wf_id = self.jqs_req.send_request({
                'action': 'get_workflows_by_inputs',
                'kwargs': {'dag_id': dag_id,
                           'workflow_args': self.workflow_args}
            })
            wf_dag[wf_id] = dag_id
        return wf_dag

    def _bind(self):
        potential_wfs = self._matching_workflows()
        if len(potential_wfs) == 1:
            self.id = [k for k in potential_wfs.keys()][0]
            self.dag_id = [v for v in potential_wfs.values()][0]
            self.task_dag.bind_to_db(self.dag_id)
        elif len(potential_wfs) == 0:
            # Bind the dag ...
            self.task_dag.bind_to_db()

            # Create new workflow in Database
            rc, wf_id = self.jsm_req.send_request({
                'action': 'add_workflow',
                'kwargs': {'dag_id': self.task_dag.dag_id,
                           'workflow_args': self.workflow_args,
                           'name': self.name,
                           'description': self.description,
                           'user': getpass.getuser()}
            })
            self.id = wf_id
        else:
            # This case should never happen... we have application side
            # protection against this, but we should probably force the
            # validation down into the DB layer as well (i.e. make the
            # dag_hash + workflow_args a unique tuple)
            # TODO: Protect against duplicated dag+workflow_args at DB level
            raise RuntimeError("Multiple matching Workflows found {}. "
                               "Workflows should be unique on TaskDag and "
                               "WorkflowArgs".format(potential_wfs))

    def _create_workflow_run(self):
        # Create new workflow in Database
        self.workflow_run = WorkflowRun(self.id)

    def execute(self):
        if not self.bound:
            self._bind()
        self._create_workflow_run()
        self.task_dag.execute()
        self.complete()

    def complete(self):
        self.workflow_run.update_complete()
        self._update_status(WorkflowStatus.COMPLETE)

    def stop(self):
        if self.is_running():
            self.kill_running_tasks()
        self.workflow_run.update_stopped()

    def is_running(self):
        # First check the database for last WorkflowRun... where we should
        # store a hostname + pid + running_flag

        # If in the database as 'running,' check the hostname
        # + pid to see if the process is actually still running:
        #   A) If so, inform the user to kill that Workflow and abort
        #   B) If not, flip the database of the previous WorkflowRun to
        #      STOPPED and create a new one
        pass

    def prompt_user(self):
        pass

    def reset_attempt_counters(self):
        pass

    def kill_running_tasks(self):
        # First check the database for any tasks that are 'running' and
        # have SGE IDs. If these are still qstat'able... qdel them
        pass

    def _update_status(self, status):
        rc, wfr_id = self.jsm_req.send_request({
            'action': 'update_workflow',
            'kwargs': {'wf_id': self.id, 'status': status}
        })
