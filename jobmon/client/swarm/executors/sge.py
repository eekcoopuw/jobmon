import json
import logging
import os
from typing import List
import subprocess
from time import sleep

import pandas as pd

from cluster_utils.io import makedirs_safely
from jobmon.client.utils import confirm_correct_perms
from jobmon.client.swarm.executors import sge_utils
from jobmon.client.swarm.executors import Executor
from jobmon.client.swarm.executors.sge_resource import SGEResource

logger = logging.getLogger(__name__)
ERROR_SGE_JID = -99999


ExectorIDs = List[int]


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
            logger.debug(f"About to qsub {qsub_cmd}")
            resp = subprocess.check_output(qsub_cmd, shell=True)
            logger.debug(f"Received from qsub {resp}")
            # Hmm, Python 2 vs 3 bug? That byte marker?
            idx = resp.split().index(b'job')
            sge_jid = int(resp.split()[idx + 1])
            return sge_jid

        except Exception as e:
            logger.error(f"Caught in qsub {e}")
            if isinstance(e, ValueError):
                raise e
            return ERROR_SGE_JID

    def execute(self, job_instance):
        return self._execute_sge(job_instance.job,
                                 job_instance.job_instance_id)

    def get_usage_stats(self):
        sge_id = os.environ.get('JOB_ID')
        usage = sge_utils.qstat_usage([sge_id])[int(sge_id)]
        return usage

    def get_actual_submitted_or_running(self) -> ExectorIDs:
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
        to_df = pd.DataFrame(data=jiid_exid_tuples,
                             columns=["job_instance_id", "executor_id"])
        if len(to_df) == 0:
            return []
        sge_jobs = sge_utils.qstat()
        sge_jobs = sge_jobs[~sge_jobs.status.isin(['hqw', 'qw'])]
        to_df = to_df.merge(sge_jobs, left_on='executor_id', right_on='job_id')
        return_list = []
        if len(to_df) > 0:
            sge_utils.qdel(list(to_df.executor_id))
            for _, row in to_df.iterrows():
                ji_id = row.job_instance_id
                hostname = row.hostname
                return_list.append((int(ji_id), hostname))
        self._poll_for_lagging_jobs(list(to_df.executor_id))
        return return_list

    def _poll_for_lagging_jobs(self, executor_ids):
        lagging_jobs = sge_utils.qstat(jids=executor_ids)
        logger.info("Qdelling executor_ids {} from a previous workflow run, "
                    "and polling to ensure they disappear from qstat"
                    .format(executor_ids))
        seconds = 0
        while seconds <= 60 and len(lagging_jobs) > 0:
            seconds += 5
            sleep(5)
            lagging_jobs = sge_utils.qstat(jids=executor_ids)
            if seconds == 60 and len(lagging_jobs) > 0:
                raise RuntimeError("Polled for 60 seconds waiting for qdel-ed "
                                   "executor_ids {} to disappear from qstat "
                                   "but they still exist. Timing out."
                                   .format(lagging_jobs.job_id.unique()))

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
            j_cmd = "-l archive"
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
