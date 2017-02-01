import logging
import os
from jobmon import sge


here = os.path.dirname(os.path.abspath(__file__))


class BaseExecutor(object):

    def __init__(self, parallelism=None):
        self.logger = logging.getLogger(__name__)

        self.parallelism = parallelism

        # track job state
        self.queued_jobs = []
        self.running_jobs = []

        # execute start method
        self.start()

    def start(self):
        pass

    def stop(self):
        pass

    def queue_job(self, job, *args, **kwargs):
        """Add a job definition to the executors queue.

        Args:
            job (jobmon.job.Job): instance of jobmon.job.Job object
        """
        self.queued_jobs.append({
            "job": job,
            "args": args,
            "kwargs": kwargs})

    def heartbeat(self):

        # Calling child class sync method
        self.logger.debug("Calling the {} sync method".format(self.__class__))
        self.sync()

        current_queue_length = len(list(self.queued_jobs))
        running_queue_length = len(self.running_jobs)
        self.logger.debug(
            "{} running job instances".format(running_queue_length))
        self.logger.debug("{} in queue".format(current_queue_length))

        # figure out how many jobs we can submit
        if not self.parallelism:
            open_slots = current_queue_length
        else:
            open_slots = self.parallelism - running_queue_length

        # submit the amount of jobs that our parallelism allows for
        for _ in range(min((open_slots, current_queue_length))):

            job_def = self.queued_jobs.pop(0)
            job = job_def.pop("job")

            # ideally execute async would return a job instance but we want to
            # reduce strain on the central job monitor so we wont construct the
            # job instance class
            job_instance_id = self.execute_async(
                monitored_jid=job.monitored_jid,
                runfile=job.runfile,
                jobname=job.name,
                parameters=job.job_args,
                *job_def["args"],
                **job_def["kwargs"])

            # add reference to job class and the executor
            job.job_instances.append(job_instance_id)
            self.running_jobs.append(job_instance_id)


class SGEExecutor(BaseExecutor):

    stub = os.path.join(here, "monitored_job.py")

    def __init__(self, mon_dir, request_retries, request_timeout,
                 path_to_conda_bin_on_target_vm, conda_env, parallelism=None):
        """
            path_to_conda_bin_on_target_vm (string, optional): which conda bin
                to use on the target vm.
            conda_env (string, optional): which conda environment you are
                using on the target vm.
        """

        super(SGEExecutor, self).__init__(parallelism)

        # environment for distributed applications
        self.mon_dir = mon_dir
        self.request_retries = request_retries
        self.request_timeout = request_timeout

        # environment for distributed applications
        self.path_to_conda_bin_on_target_vm = path_to_conda_bin_on_target_vm
        self.conda_env = conda_env

    def execute_async(self, monitored_jid, runfile, jobname, parameters=[],
                      *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. They will automatically
        register with server and sqlite database.

        Args:
            monitored_jid (int): what id to use for this job in the jobmon
                database.
            runfile (sting): full path to python executable file.
            jobname (sting): what name to register the sge job under.
            parameters (list, optional): command line arguments to be passed
                into runfile.

            see sge.qsub for further options

        Returns:
            sge job id
        """
        base_params = [
            "--mon_dir", self.mon_dir,
            "--runfile", runfile,
            "--monitored_jid", monitored_jid,
            "--request_retries", self.request_retries,
            "--request_timeout", self.request_timeout
        ]

        # replace -- with ## to allow for passthrough in monitored job
        passed_params = []
        for param in parameters:
            passed_params.append(str(param).replace("--", "##", 1))

        # append additional parameters
        if parameters:
            parameters = base_params + passed_params
        else:
            parameters = base_params

        self.logger.debug(
            ("{}: Submitting job to qsub:"
             " runfile {}; jobname {}; parameters {}; path: {}"
             ).format(os.getpid(),
                      self.stub,
                      jobname,
                      parameters,
                      self.path_to_conda_bin_on_target_vm))
        # submit.
        sgeid = sge.qsub(
            runfile=self.stub,
            jobname=jobname,
            prepend_to_path=self.path_to_conda_bin_on_target_vm,
            conda_env=self.conda_env,
            parameters=parameters,
            *args, **kwargs)

        # ideally would create an sge job instance here and return it instead
        # of the id
        return sgeid

    def sync(self):

        # check state of all jobs currently in sge queue
        self.logger.debug('{}: Polling jobs...'.format(os.getpid()))
        current_jobs = set(sge.qstat(jids=self.running_jobs).job_id.tolist())
        self.logger.debug('             ... ' +
                          str(len(current_jobs)) + ' active jobs')
        self.running_jobs = list(current_jobs)


class SequentialExecutor(BaseExecutor):
    pass
