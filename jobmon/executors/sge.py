import json
import logging
import os
import subprocess
from time import sleep

import pandas as pd

from cluster_utils.io import makedirs_safely
from jobmon import sge
from jobmon.executors import Executor


logger = logging.getLogger(__name__)
ERROR_SGE_JID = -99999


class SGEExecutor(Executor):

    def __init__(self, stderr=None, stdout=None, project=None,
                 working_dir=None, *args, **kwargs):
        self.stderr = stderr
        self.stdout = stdout
        self.project = project
        self.working_dir = working_dir

        super().__init__(*args, **kwargs)

    def _execute_sge(self, job, job_instance_id):
        try:
            qsub_cmd = self.build_wrapped_command(job, job_instance_id,
                                                  self.stderr, self.stdout,
                                                  self.project,
                                                  self.working_dir)
            resp = subprocess.check_output(qsub_cmd, shell=True)
            idx = resp.split().index(b'job')
            sge_jid = int(resp.split()[idx + 1])

            # TODO: FIX THIS ... DRMAA QSUB METHOD IS FAILING FOR SOME REASON,
            # NEED TO INVESTIGATE THE JOBTYPE ASSUMPTIONS. RESORTING TO
            # BASIC COMMAND-LINE QSUB FOR NOW
            # sge_jid = sge.qsub(cmd, jobname=job.name, jobtype='plain')

            return sge_jid
        except Exception as e:
            logger.error(e)
            return ERROR_SGE_JID

    def execute(self, job_instance):
        return self._execute_sge(job_instance.job,
                                 job_instance.job_instance_id)

    def get_usage_stats(self):
        sge_id = os.environ.get('JOB_ID')
        usage = sge.qstat_usage([sge_id])[int(sge_id)]
        return usage

    def get_actual_submitted_or_running(self):
        qstat_out = sge.qstat()
        executor_ids = list(qstat_out.job_id)
        executor_ids = [int(eid) for eid in executor_ids]
        return executor_ids

    def terminate_job_instances(self, job_instance_list):
        ji_dict_list = [ji.to_wire() for ji in job_instance_list]
        to_df = pd.DataFrame.from_dict(ji_dict_list)
        if len(to_df) == 0:
            return []
        sge_jobs = sge.qstat()
        sge_jobs = sge_jobs[~sge_jobs.status.isin(['hqw', 'qw'])]
        to_df = to_df.merge(sge_jobs, left_on='executor_id', right_on='job_id')
        return_list = []
        if len(to_df) > 0:
            sge.qdel(list(to_df.executor_id))
            for _, row in to_df.iterrows():
                ji_id = row.job_instance_id
                hostname = row.hostname
                return_list.append((int(ji_id), hostname))
        self._poll_for_lagging_jobs(list(to_df.executor_id))
        return return_list

    def _poll_for_lagging_jobs(self, executor_ids):
        lagging_jobs = sge.qstat(jids=executor_ids)
        logger.info("Qdelling executor_ids {} from a previous workflow run, "
                    "and polling to ensure they disappear from qstat"
                    .format(executor_ids))
        seconds = 0
        while seconds <= 60 and len(lagging_jobs) > 0:
            seconds += 5
            sleep(5)
            lagging_jobs = sge.qstat(jids=executor_ids)
            if seconds == 60 and len(lagging_jobs) > 0:
                raise RuntimeError("Polled for 60 seconds waiting for qdel-ed "
                                   "executor_ids {} to disappear from qstat "
                                   "but they still exist. Timing out."
                                   .format(lagging_jobs.job_id.unique()))

    def build_wrapped_command(self, job, job_instance_id, stderr=None,
                              stdout=None, project=None, working_dir=None):
        """Process the Job's context_args, which are assumed to be
        a json-serialized dictionary"""
        # TODO: Settle on a sensible way to pass and validate settings for the
        # command's context (i.e. context = Executor, SGE/Sequential/Multiproc)

        ctx_args = json.loads(job.context_args)
        if 'sge_add_args' in ctx_args:
            sge_add_args = ctx_args['sge_add_args']
        else:
            sge_add_args = ""
        if project:
            project_cmd = "-P {}".format(project)
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
        base_cmd = super().build_wrapped_command(job, job_instance_id)
        thispath = os.path.dirname(os.path.abspath(__file__))

        # NOTE: The -V or equivalent is critical here to propagate the value of
        # the JOBMON_CONFIG environment variable to downstream Jobs...
        # otherwise those Jobs could end up using a different config and not be
        # able to talk back to the appropriate server(s)
        qsub_cmd = ('qsub {wd} -N {jn} '
                    '-pe multi_slot {slots} -l mem_free={mem}g '
                    '{project} {stderr} {stdout} '
                    '{sge_add_args} '
                    '-V {path}/submit_master.sh '
                    '"{cmd}"'.format(
                        wd=wd_cmd,
                        jn=job.name,
                        slots=job.slots,
                        mem=job.mem_free,
                        sge_add_args=sge_add_args,
                        path=thispath,
                        cmd=base_cmd,
                        project=project_cmd,
                        stderr=stderr_cmd,
                        stdout=stdout_cmd))
        return qsub_cmd
