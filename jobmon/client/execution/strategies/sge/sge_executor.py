import os
from subprocess import check_output
from typing import List, Tuple, Dict, Optional

from cluster_utils.io import makedirs_safely

from jobmon.client import ClientLogging as logging
from jobmon.client.execution.strategies.base import (
    Executor, TaskInstanceExecutorInfo, ExecutorParameters)
from jobmon.client.execution.strategies.sge import sge_utils
from jobmon.exceptions import RemoteExitInfoNotAvailable, ReturnCodes
from jobmon.models.task_instance_status import TaskInstanceStatus
from jobmon.models.attributes.constants import qsub_attribute

logger = logging.getLogger(__name__)

ERROR_SGE_JID = -99999
ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES = (137, 247, -9)


class SGEExecutor(Executor):
    def __init__(self,
                 stderr: Optional[str] = None,
                 stdout: Optional[str] = None,
                 project: Optional[str] = None,
                 working_dir: Optional[str] = None,
                 *args, **kwargs) -> None:
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir
        super().__init__(*args, **kwargs)

    def _execute_sge(self, qsub_cmd) -> int:
        try:
            logger.debug(f"Qsub command is: {qsub_cmd}")
            resp = check_output(qsub_cmd, shell=True, universal_newlines=True)
            if 'Your job' in resp:
                idx = resp.split().index('job')
                sge_jid = int(resp.split()[idx + 1])
            elif 'no suitable queue' in resp:
                logger.error(f"The job could not be submitted as requested. Got SGE error "
                             f"{resp}. Tried submitting {qsub_cmd}")
                sge_jid = qsub_attribute.NO_EXEC_ID
            else:
                logger.error(f"The qsub was successfully submitted, but the "
                             f"job id could not be parsed from the response: "
                             f"{resp}")
                sge_jid = qsub_attribute.UNPARSABLE
            return sge_jid

        except Exception as e:
            logger.error(
                f"Error in {self.__class__.__name__} while running {qsub_cmd}:"
                f"\n{e}")
            if isinstance(e, ValueError):
                raise e
            return qsub_attribute.NO_EXEC_ID

    def execute(self, command: str, name: str,
                executor_parameters: ExecutorParameters) -> int:
        logger.debug(f"PARAMS: {executor_parameters.m_mem_free}, "
                     f"{executor_parameters.num_cores}, "
                     f"{executor_parameters.queue},"
                     f" {executor_parameters.max_runtime_seconds}, "
                     f"{executor_parameters.j_resource},"
                     f" {executor_parameters.context_args}")
        qsub_command = self._build_qsub_command(
            base_cmd=self.jobmon_command + " " + command,
            name=name,
            mem=executor_parameters.m_mem_free,
            cores=executor_parameters.num_cores,
            queue=executor_parameters.queue,
            runtime=executor_parameters.max_runtime_seconds,
            j=executor_parameters.j_resource,
            context_args=executor_parameters.context_args,
            stderr=self.stderr,
            stdout=self.stdout,
            project=self.project,
            working_dir=self.working_dir)
        logger.info(qsub_command)
        return self._execute_sge(qsub_command)

    def get_actual_submitted_or_running(self) -> List[int]:
        qstat_dict = sge_utils.qstat()
        executor_ids = list(qstat_dict.keys())
        executor_ids = [int(eid) for eid in executor_ids]
        return executor_ids

    def terminate_task_instances(self, executor_ids: List[int]) -> None:
        """Only terminate the task instances that are running, not going to
        kill the jobs that are actually still in a waiting or a transitioning
        state"""
        logger.debug(f"Going to terminate: {executor_ids}")
        sge_utils.qdel(executor_ids)

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        """return the exit state associated with a given exit code"""
        exit_code, reason = sge_utils.qacct_exit_status(executor_id)
        logger.debug(f"exit_status info: {exit_code}")
        if exit_code in ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES:
            if 'over runtime' in reason:
                msg = ("Task Instance killed because it exceeded max_runtime. "
                       f"{self.__class__.__name__} accounting discovered exit "
                       f"code:{exit_code}.")
            else:
                msg = ("Insufficient resources requested. Task was lost. "
                       f"{self.__class__.__name__} accounting discovered exit "
                       f"code:{exit_code}.")
            return TaskInstanceStatus.RESOURCE_ERROR, msg
        elif exit_code == ReturnCodes.WORKER_NODE_ENV_FAILURE:
            msg = (
                "There is a discrepancy between the environment that your "
                "workflow swarm node is accessing and the environment that "
                "your worker node is accessing, because of this they will "
                "not be able to access the correct jobmon services."
                " Please check that they are accessing the environments "
                "as expected (check qsub that was submitted for hints). "
                "CHECK YOUR BASH PROFILE as it may contain a path that "
                "references a different version of jobmon than you intend "
                f"to use. {self.__class__.__name__} accounting discovered "
                f"exit code: {exit_code}")
            # TODO change this to a fatal error so they can't attempt a retry
            return TaskInstanceStatus.UNKNOWN_ERROR, msg
        else:
            raise RemoteExitInfoNotAvailable

    def _build_qsub_command(self,
                            base_cmd: str,
                            name: str,
                            mem: float,
                            cores: int,
                            queue: str,
                            runtime: int,
                            j: bool,
                            context_args: dict,
                            stderr: Optional[str] = None,
                            stdout: Optional[str] = None,
                            project: Optional[str] = None,
                            working_dir: Optional[str] = None
                            ) -> str:
        """Process the Task's context_args, which are assumed to be
        a json-serialized dictionary
        """

        sge_add_args = ""
        if context_args:
            if 'sge_add_args' in context_args:
                sge_add_args = context_args['sge_add_args']

        if project:
            project_cmd = f"-P {project}"
        elif not project:
            project_cmd = "-P ihme_general"
        else:
            project_cmd = ""
        if stderr:
            stderr_cmd = f"-e {stderr}"
            makedirs_safely(stderr)
        else:
            stderr_cmd = ""
        if stdout:
            stdout_cmd = f"-o {stdout}"
            makedirs_safely(stdout)
        else:
            stdout_cmd = ""
        if working_dir:
            wd_cmd = f"-wd {working_dir}"
        else:
            wd_cmd = ""
        if mem:
            mem_cmd = f"-l m_mem_free={mem}G"
        elif mem:
            mem_cmd = f"-l mem_free={mem}G"
        else:
            mem_cmd = ""
        if cores:
            cpu_cmd = f"-l fthread={cores}"
        else:
            cpu_cmd = f"-pe multi_slot {cores}"
        if j is True:
            j_cmd = "-l archive=TRUE"
        else:
            j_cmd = ""
        if queue:
            q_cmd = f"-q '{queue}'"
        else:
            # The 'new' cluster requires a queue name be passed
            # explicitly, so in the event the user does not supply one we just
            # fall back to all.q
            q_cmd = "-q all.q"
        if runtime:
            time_cmd = f"-l h_rt={runtime}"
        else:
            time_cmd = ""

        thispath = os.path.dirname(os.path.abspath(__file__))

        # NOTE: The -V or equivalent is critical here to propagate the value of
        # the JOBMON_CONFIG environment variable to downstream Jobs...
        # otherwise those Jobs could end up using a different config and not be
        # able to talk back to the appropriate server(s)
        qsub_cmd = ('qsub {wd} -N {jn} {qc} '
                    '{cpu} {j} {mem} {time} '
                    '{project} {stderr} {stdout} '
                    '-w e '
                    '{sge_add_args} '
                    '-V {path}/submit_master.sh '
                    '"{cmd}"'.format(
                        wd=wd_cmd,
                        qc=q_cmd,
                        jn=name,
                        cpu=cpu_cmd,
                        j=j_cmd,
                        mem=mem_cmd,
                        time=time_cmd,
                        sge_add_args=sge_add_args,
                        path=thispath,
                        cmd=base_cmd,
                        project=project_cmd,
                        stderr=stderr_cmd,
                        stdout=stdout_cmd))
        return qsub_cmd


class TaskInstanceSGEInfo(TaskInstanceExecutorInfo):

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            sge_jid = os.environ.get('JOB_ID')
            if sge_jid:
                self._executor_id = int(sge_jid)
        logger.info("executor_id: {}".format(self._executor_id))
        return self._executor_id

    def get_usage_stats(self) -> Dict:
        return sge_utils.qstat_usage([self.executor_id])[self.executor_id]

    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        if exit_code in ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES:
            msg = (f"Insufficient resources requested. Found exit code: "
                   f"{exit_code}. Application returned error message:\n" +
                   error_msg)
            return TaskInstanceStatus.RESOURCE_ERROR, msg
        elif exit_code == ReturnCodes.WORKER_NODE_ENV_FAILURE:
            msg = (
                "There is a discrepancy between the environment that your "
                "workflow swarm node is accessing and the environment that "
                "your worker node is accessing, because of this they will "
                "not be able to access the correct jobmon services."
                " Please check that they are accessing the environments "
                "as expected (check qsub that was submitted for hints). "
                "CHECK YOUR BASH PROFILE as it may contain a path that "
                "references a different version of jobmon than you intend "
                f"to use. {self.__class__.__name__} accounting discovered "
                f"exit code: {exit_code}")
            # TODO change this to a fatal error so they can't attempt a retry
            return TaskInstanceStatus.UNKNOWN_ERROR, msg
        else:
            return TaskInstanceStatus.ERROR, error_msg



