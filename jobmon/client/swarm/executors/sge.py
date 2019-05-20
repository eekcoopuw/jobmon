import json
import logging
import os
from subprocess import check_output
from typing import List
import traceback

import pandas as pd

from cluster_utils.io import makedirs_safely

from jobmon.client import shared_requester
from jobmon.client.utils import confirm_correct_perms
from jobmon.client.swarm.executors import sge_utils
from jobmon.client.swarm.executors import Executor, ExecutorWorkerNode
from jobmon.client.swarm.executors.sge_resource import SGEResource
from jobmon.exceptions import RemoteExitInfoNotAvailable
from jobmon.models.job_instance_status import JobInstanceStatus
from jobmon.models.attributes.constants import qsub_attribute


logger = logging.getLogger(__name__)

ERROR_SGE_JID = -99999
ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES = (137, 247, 44, -9)

ExecutorIDs = List[int]


class SGEExecutor(Executor):

    def __init__(self, stderr=None, stdout=None, project=None,
                 working_dir=None, *args, **kwargs):
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir

        super().__init__(*args, **kwargs)

        confirm_correct_perms()

    def _execute_sge(self, job, job_instance_id):
        try:
            qsub_cmd = self.build_wrapped_command(job, job_instance_id,
                                                  self.stderr, self.stdout,
                                                  self.project,
                                                  self.working_dir)
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
            msg = (
                f"Error in executor {self.__class__.__name__}, {str(self)} "
                f"while submitting ji_id {job_instance_id}: \n{stack}")
            shared_requester.send_request(
                app_route="/error_logger",
                message={"traceback": msg},
                request_type="post")
            if isinstance(e, ValueError):
                raise e
            return qsub_attribute.NO_EXEC_ID

    def execute(self, job_instance):
        return self._execute_sge(job_instance.job,
                                 job_instance.job_instance_id)

    def get_usage_stats(self):
        sge_id = os.environ.get('JOB_ID')
        usage = sge_utils.qstat_usage([sge_id])[int(sge_id)]
        return usage

    def get_actual_submitted_or_running(self) -> ExecutorIDs:
        qstat_out = sge_utils.qstat()
        executor_ids = list(qstat_out.job_id)
        executor_ids = [int(eid) for eid in executor_ids]
        return executor_ids

    def get_actual_submitted_to_executor(self):
        """get jobs that qstat thinks are submitted but not yet running."""

        # jobs returned by this function may well be actually running or done,
        # but those state transitions are handled by the worker node/heartbeat.
        qstat_out = sge_utils.qstat(status='pr')
        qstat_out = qstat_out[
            qstat_out.status.isin(["qw", "hqw", "hRwq", "t"])]
        executor_ids = list(qstat_out.job_id)
        executor_ids = [int(eid) for eid in executor_ids]
        return executor_ids

    def terminate_job_instances(self, jiid_exid_tuples):
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

    def get_remote_exit_info(self, executor_id):
        """return the exit state associated with a given exit code"""
        exit_code = sge_utils.qacct_exit_status(executor_id)
        if exit_code in ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES:
            msg = ("Insufficient resources requested. Job was lost. "
                   f"{self.__class__.__name__} accounting discovered exit code"
                   f":{exit_code}.")
            return JobInstanceStatus.RESOURCE_ERROR, msg
        else:
            raise RemoteExitInfoNotAvailable

    def build_wrapped_command(self, job, job_instance_id, stderr=None,
                              stdout=None, project=None, working_dir=None):
        """Process the Job's context_args, which are assumed to be
        a json-serialized dictionary
        """
        resources = SGEResource(slots=job.slots, mem_free=job.mem_free,
                                num_cores=job.num_cores, queue=job.queue,
                                max_runtime_seconds=job.max_runtime_seconds,
                                j_resource=job.j_resource)

        # if the job is configured for the fair cluster, but is being run on
        # dev/prod we need to make sure it formats its qsub to work on dev/prod
        dev_or_prod = False
        # el6 means it's dev or prod
        if "el6" in os.environ['SGE_ENV']:
            dev_or_prod = True

        (mem_free, num_cores, queue, max_runtime,
         j_resource) = resources.return_valid_resources()

        ctx_args = json.loads(job.context_args)
        if 'sge_add_args' in ctx_args:
            sge_add_args = ctx_args['sge_add_args']
        else:
            sge_add_args = ""
        if project:
            project_cmd = "-P {}".format(project)
        elif not dev_or_prod and not project:
            project_cmd = "-P ihme_general"
        else:
            project_cmd = ""
        if stderr:
            stderr_cmd = "-e {}".format(stderr)
            makedirs_safely(stderr)
        else:
            stderr_cmd = ""
        if stdout:
            stdout_cmd = "-o {}".format(stdout)
            makedirs_safely(stdout)
        else:
            stdout_cmd = ""
        if working_dir:
            wd_cmd = "-wd {}".format(working_dir)
        else:
            wd_cmd = ""
        if mem_free and not dev_or_prod:
            mem_cmd = "-l m_mem_free={}G".format(mem_free)
        elif mem_free:
            mem_cmd = "-l mem_free={}G".format(mem_free)
        else:
            mem_cmd = ""
        if num_cores and not dev_or_prod:
            cpu_cmd = "-l fthread={}".format(num_cores)
        else:
            cpu_cmd = "-pe multi_slot {}".format(num_cores)
        if j_resource is True and not dev_or_prod:
            j_cmd = "-l archive=TRUE"
        else:
            j_cmd = ""
        if queue and not dev_or_prod:
            q_cmd = "-q '{}'".format(queue)
        elif not dev_or_prod and queue is None:
            # The 'new' cluster requires a queue name be passed
            # explicitly, so in the event the user does not supply one we just
            # fall back to all.q
            q_cmd = "-q all.q"
        else:
            q_cmd = ""
        if max_runtime and not dev_or_prod:
            time_cmd = "-l h_rt={}".format(max_runtime)
        else:
            time_cmd = ""

        base_cmd = super().build_wrapped_command(job, job_instance_id)
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
                        jn=job.name,
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


class SGEExecutorWorkerNode(ExecutorWorkerNode):

    def __init__(self):
        self._executor_id = os.environ.get('JOB_ID')

    def executor_id(self):
        if self._executor_id is None:
            self._executor_id = os.environ.get('JOB_ID')
        return self._executor_id

    def get_usage_stats(self):
        return sge_utils.qstat_usage([self.executor_id])[int(self.executor_id)]

    def get_exit_info(self, exit_code, error_msg):
        if exit_code in ERROR_CODE_SET_KILLED_FOR_INSUFFICIENT_RESOURCES:
            msg = (f"Insufficient resources requested. Found exit code: "
                   f"{exit_code}. Application returned error message:\n" +
                   error_msg)
            return JobInstanceStatus.RESOURCE_ERROR, msg
        else:
            return JobInstanceStatus.ERROR, error_msg
