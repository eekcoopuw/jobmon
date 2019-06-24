import logging
import os
from subprocess import check_output
import traceback
from typing import List, Tuple, Dict, Optional

import pandas as pd

from cluster_utils.io import makedirs_safely

from jobmon.client import shared_requester
from jobmon.client.utils import confirm_correct_perms
from jobmon.client.swarm.executors import (Executor, JobInstanceExecutorInfo,
                                           sge_utils, ExecutorParameters)
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.attributes.constants import qsub_attribute

logger = logging.getLogger(__name__)

ERROR_SGE_JID = -99999
ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES = (137, 247, -9)
ERROR_CODE_KILLED_FOR_ENV_ERR = 198


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

        confirm_correct_perms()

    def _execute_sge(self, qsub_cmd) -> int:
        try:
            logger.debug(f"Qsub command is: {qsub_cmd}")
            resp = check_output(qsub_cmd, shell=True, universal_newlines=True)
            logger.debug(f"****** Received from qsub '{resp}'")
            if 'job' in resp:
                idx = resp.split().index('job')
                sge_jid = int(resp.split()[idx + 1])
            else:
                logger.error(f"The qsub was successfully submitted, but the "
                             f"job id could not be parsed from the response: "
                             f"{resp}")
                sge_jid = qsub_attribute.UNPARSABLE
            return sge_jid

        except Exception as e:
            stack = traceback.format_exc()
            logger.error(f"*** Caught during qsub {e}")
            logger.error(f"Traceback {stack}")
            logger.error("qsub response: {resp}")
            msg = (
                f"Error in executor {self.__class__.__name__}, {str(self)} "
                f"while executing command {qsub_cmd}: \n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")
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
            base_cmd=command,
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
        return self._execute_sge(qsub_command)

    def get_actual_submitted_or_running(self) -> List[int]:
        qstat_out = sge_utils.qstat()
        executor_ids = list(qstat_out.job_id)
        executor_ids = [int(eid) for eid in executor_ids]
        return executor_ids

    def terminate_job_instances(self, jiid_exid_tuples: List[Tuple[int, int]]
                                ) -> List[Tuple[int, str]]:
        """Only terminate the job instances that are running, not going to
        kill the jobs that are actually still in a waiting or transitioning
        state"""
        to_df = pd.DataFrame(data=jiid_exid_tuples,
                             columns=["job_instance_id", "executor_id"])
        if len(to_df) == 0:
            return []
        sge_jobs = sge_utils.qstat()
        sge_jobs = sge_jobs[~sge_jobs.status.isin(['hqw', 'qw', "hRwq", "t"])]
        to_df = to_df.merge(sge_jobs, left_on='executor_id', right_on='job_id')
        return_list = []
        if len(to_df) > 0:
            sge_utils.qdel(list(to_df.executor_id))
            for _, row in to_df.iterrows():
                ji_id = row.job_instance_id
                hostname = row.hostname
                return_list.append((int(ji_id), hostname))
        return return_list

    def get_remote_exit_info(self, executor_id: int) -> Tuple[str, str]:
        """return the exit state associated with a given exit code"""
        exit_code = sge_utils.qacct_exit_status(executor_id)
        if exit_code in ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES:
            msg = ("Insufficient resources requested. Job was lost. "
                   f"{self.__class__.__name__} accounting discovered exit code"
                   f":{exit_code}.")
            return JobInstanceStatus.RESOURCE_ERROR, msg
        elif exit_code == ERROR_CODE_KILLED_FOR_ENV_ERR:
            msg = "There is a discrepancy between the environment that your " \
                  "workflow swarm node is accessing and the environment that " \
                  "your worker node is accessing, because of this they will " \
                  "not be able to access the correct jobmon services." \
                  " Please check that they are accessing the environments " \
                  "as expected (check qsub that was submitted for hints). " \
                  "CHECK YOUR BASH PROFILE as it may contain a path that " \
                  "references a different version of jobmon than you intend " \
                  f"to use. {self.__class__.__name__} accounting discovered " \
                  f"exit code: {exit_code}"
            # TODO change this to a fatal error so they can't attempt a retry
            return JobInstanceStatus.ERROR, msg
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
        """Process the Job's context_args, which are assumed to be
        a json-serialized dictionary
        """

        dev_or_prod = False
        # el6 means it's dev or prod
        if "el6" in os.environ['SGE_ENV']:
            dev_or_prod = True

        sge_add_args = ""
        if context_args:
            if 'sge_add_args' in context_args:
                sge_add_args = context_args['sge_add_args']

        if project:
            project_cmd = f"-P {project}"
        elif not dev_or_prod and not project:
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
        if mem and not dev_or_prod:
            mem_cmd = f"-l m_mem_free={mem}G"
        elif mem:
            mem_cmd = f"-l mem_free={mem}G"
        else:
            mem_cmd = ""
        if cores and not dev_or_prod:
            cpu_cmd = f"-l fthread={cores}"
        else:
            cpu_cmd = f"-pe multi_slot {cores}"
        if j is True and not dev_or_prod:
            j_cmd = "-l archive=TRUE"
        else:
            j_cmd = ""
        if queue and not dev_or_prod:
            q_cmd = f"-q '{queue}'"
        elif not dev_or_prod and queue is None:
            # The 'new' cluster requires a queue name be passed
            # explicitly, so in the event the user does not supply one we just
            # fall back to all.q
            q_cmd = "-q all.q"
        else:
            q_cmd = ""
        if runtime and not dev_or_prod:
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


class JobInstanceSGEInfo(JobInstanceExecutorInfo):

    def __init__(self) -> None:
        self._executor_id: Optional[int] = None

    @property
    def executor_id(self) -> Optional[int]:
        if self._executor_id is None:
            jid = os.environ.get('JOB_ID')
            if jid:
                self._executor_id = int(jid)
        return self._executor_id

    def get_usage_stats(self) -> Dict:
        return sge_utils.qstat_usage([self.executor_id])[self.executor_id]

    def get_exit_info(self, exit_code: int, error_msg: str) -> Tuple[str, str]:
        if exit_code in ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES:
            msg = (f"Insufficient resources requested. Found exit code: "
                   f"{exit_code}. Application returned error message:\n" +
                   error_msg)
            return JobInstanceStatus.RESOURCE_ERROR, msg
        elif exit_code == ERROR_CODE_KILLED_FOR_ENV_ERR:
            msg = "There is a discrepancy between the environment that your " \
                  "workflow swarm node is accessing and the environment that " \
                  "your worker node is accessing, because of this they will " \
                  "not be able to access the correct jobmon services." \
                  " Please check that they are accessing the environments " \
                  "as expected (check qsub that was submitted for hints). " \
                  "CHECK YOUR BASH PROFILE as it may contain a path that " \
                  "references a different version of jobmon than you intend " \
                  f"to use. {self.__class__.__name__} accounting discovered " \
                  f"exit code: {exit_code}"
            # TODO change this to a fatal error so they can't attempt a retry
            return JobInstanceStatus.ERROR, msg
        else:
            return JobInstanceStatus.ERROR, error_msg
