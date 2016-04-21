import os
import time
import warnings
from datetime import datetime
from . import sge, job


this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))


class IgnorantQ(object):
    """keep track of jobs submitted to sun grid engine"""

    def __init__(self):
        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}

    def qsub(self, *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. see sge module for
        documentation.

        Returns:
            sge job id
        """
        sgeid = sge.qsub(*args, **kwargs)
        self.scheduled_jobs.append(sgeid)
        self.jobs[sgeid] = {"args": args, "kwargs": kwargs}
        return sgeid

    def qblock(self, poll_interval=10):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue. check sge queue each poll_interval until q is
        clear. Run qmanage each poll_interval.

        Args:
            poll_interval (int, optional): time in seconds between each
            qcomplete()
        """
        while not self.qcomplete():
            time.sleep(poll_interval)

    def qmanage(self):
        """run manage_exit_q and manage_current_q based on the changes to those
        queues between the current qmanage call and the previous call"""
        print 'Polling jobs ... {}'.format(datetime.now())

        # manage all jobs currently in sge queue
        current_jobs = set(sge.qstat(jids=self.scheduled_jobs
                                     ).job_id.tolist())
        print '             ... ' + str(len(current_jobs)) + ' active jobs'
        self.manage_current_q(current_jobs)

        # manage each job that has left the sge q
        scheduled_jobs = set(self.scheduled_jobs)
        exit_jobs = list(scheduled_jobs - current_jobs)
        for sgeid in exit_jobs:
            self.scheduled_jobs.remove(sgeid)
        self.manage_exit_q(exit_jobs)

    def qcomplete(self):
        """check if qmaster instance has done all possible work currently"""
        self.qmanage()
        return len(self.scheduled_jobs) == 0

    def manage_exit_q(self, exit_jobs):
        """action to take when job has left the q. delete job from jobs dict
        del(self.jobs[jid])

        Args:
            exit_jobs (list): list of sge job ids that have left the queue
        """
        pass

    def manage_current_q(self, current_jobs):
        """action to take on any job in the sge queue. For ignorant q, there
        is no management by default. override to add custom handling

        Args:
            current_jobs (list): list of sge job ids that are currently in the
                sge queue
        """
        pass


class MonitoredQ(IgnorantQ):
    """monitored Q supports monitoring of a single job queue by using a sqlite
    back monitoring server that all sge jobs automatically connect to after
    they get scheduled."""

    def __init__(self, out_dir, resubmits=0, prepend_to_path=None,
                 conda_env=None, request_timeout=None, request_retries=None):
        """
        Args:
            out_dir (string): directory where monitor server is running
            resubmits (int, optional): how many times to resubmit failed jobs
            prepend_to_path (string, optional): which conda bin you are using.
                only use if MonitoredQ can't figure it out on it's own.
            conda_env (string, optional): which conda environment you are
                using. only use if MonitoredQ can't figure it out on it's own.
        """
        self.out_dir = out_dir
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.resubmits = resubmits

        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}

        # environment stuffs
        python = sge.true_path(executable="python")
        if prepend_to_path is not None and conda_env is not None:
            self.prepend_to_path = prepend_to_path
            self.conda_env = conda_env
            assert conda_env in python, ("specified env does not match current"
                                         " python path")
            self.runfile = sge.true_path(executable="monitored_job.py")
        else:
            if "/.conda/" in python:
                self.prepend_to_path = "/usr/local/software/anaconda/bin"
                self.conda_env = python.split("/")[-3]
                self.runfile = sge.true_path(executable="monitored_job.py")
            elif "/ihme/code/central_comp/anaconda" in python:
                self.prepend_to_path = "/ihme/code/central_comp/anaconda/bin"
                self.conda_env = python.split("/")[-3]
                self.runfile = sge.true_path(executable="monitored_job.py")
            else:
                raise Exception("unable to determine conda env")

        # internal server manager
        self.manager = job.ManageJobMonitor(
            out_dir, request_retries=self.request_retries,
            request_timeout=self.request_timeout)

        # make sure server is booted
        if not self.manager.isalive():
            try:
                self.start_monitor(self.out_dir)
            except job.ServerRunning:
                pass
            except job.ServerStartLocked:
                time.sleep(15)
            finally:
                if not self.manager.isalive():
                    warnings.warn("could not start jobmonitor server")

    def start_monitor(self, out_dir,
                      prepend_to_path="/ihme/code/central_comp/anaconda/bin",
                      conda_env="jobmon35", restart=False, nolock=False):
        """start a jobmonitor server in a subprocess. MonitoredQ's share
        monitor servers and auto increments if they are initialized with the
        same out_dir in the same python instance.

        Args:
            out_dir (string): full path to directory where logging will happen
            prepend_to_path (string, optional): anaconda bin to prepend to path
            conda_env (string, optional): python >= 3.5 conda env to run server
                in.
            restart (bool, optional): whether to force a new server instance to
                start. Will shutdown existing server instance if one exists.
            nolock (bool, optional): ignore any boot locks for the specified
                directory. Highly not recommended.

        Returns:
            Boolean whether the server started successfully or not.
        """
        prepend_to_path = sge.true_path(file_or_dir=prepend_to_path)
        self.manager.start_server(prepend_to_path=prepend_to_path,
                                  conda_env=conda_env, restart=restart,
                                  nolock=nolock)

    def stop_monitor(self):
        """stop jobmonitor server tied to this MonitoredQ instance"""
        if self.manager.isalive():
            self.manager.stop_server()

    def qsub(self, runfile, jobname, jid=None, parameters=[], *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. They will automatically
        register with server and sqlite database.

        Args:
            runfile (sting): full path to python executable file.
            jobname (sting): what name to register the sge job under.
            jid (int, optional): what id to use for this job in the
                jobmon database. by default will auto increment using get_i().
            parameters (list, optional): command line arguments to be passed
                into runfile.

            see sge.qsub for further options

        Returns:
            sge job id
        """

        # configure arguments for parsing by ./bin/monitored_job.py
        if jid is None:
            msg = {'action': 'create_job', 'args': ''}
            r = self.manager.send_request(msg)
            jid = r[1]
        base_params = ["--mon_dir", self.out_dir, "--runfile", runfile,
                       "--jid", jid]
        if self.request_timeout is not None:
            base_params.extend(["--request_timeout", self.request_timeout])
        if self.request_retries is not None:
            base_params.extend(["--request_retries", self.request_retries])

        # replace -- with ## to allow for passthrough in monitored job
        passed_params = []
        for param in parameters:
            passed_params.append(str(param).replace("--", "##", 1))

        # append additional parameters
        if parameters:
            parameters = base_params + passed_params
        else:
            parameters = base_params

        # submit.
        sgeid = sge.qsub(runfile=self.runfile, jobname=jobname,
                         prepend_to_path=self.prepend_to_path,
                         conda_env=self.conda_env,
                         jobtype=None, parameters=parameters, *args, **kwargs)

        # update database to reflect submitted status
        msg = {'action': 'update_job_status', 'args': [jid, 2]}
        self.manager.send_request(msg)

        # store submission params in self.jobs dict in case of resubmit
        self.scheduled_jobs.append(sgeid)
        self.jobs[jid] = {"runfile": runfile,
                          "jobname": jobname,
                          "parameters": parameters,
                          "args": args,
                          "kwargs": kwargs}
        return sgeid

    def manage_exit_q(self, exit_jobs):
        """custom exit queue management. Jobs that have logged a failed state
        automagically resubmit themselves 'retries' times. default is 0.

        Args:
            exit_jobs (int): sge job id of any jobs that have left the queue
                between concurrent qmanage() calls
        """

        query_status = """
        SELECT
            current_status, jid
        FROM
            job
        JOIN
            sgejob USING (jid)
        WHERE
        """
        query_failed = """
        SELECT
            COUNT(*) as num,
            jid
        FROM
            job
        JOIN
            sgejob USING (jid)
        JOIN
            job_status USING (jid)
        WHERE
            status = 3"""

        for sgeid in exit_jobs:
            result = self.manager.query(
                query_status + "sgeid = {sgeid};".format(sgeid=sgeid))[1]
            try:
                current_status = result["current_status"].item()
                jid = result["jid"].item()
            except ValueError:
                current_status = None

            if current_status is None:
                warnings.warn(("sge job {id} left the sge queue without "
                               "registering in 'sgejob' table"
                               ).format(id=sgeid))
            if current_status == 1:
                warnings.warn(("sge job {id} left the sge queue without "
                               "ever changing status to submitted. This is "
                               "highly unlikely.").format(id=sgeid))
            elif current_status == 2:
                warnings.warn(("sge job {id} left the sge queue without "
                               "starting job execution. this is probably "
                               "bad.").format(id=sgeid))
            elif current_status == 3:
                warnings.warn(("sge job {id} left the sge queue after "
                               "starting job execution. but did not "
                               "register an error and did not register "
                               "completed. This is probably bad."
                               ).format(id=sgeid))
            elif current_status == 4:
                fails = self.manager.query(
                    query_failed + " AND sgeid = {id};".format(id=sgeid))[1]
                if fails["num"].item() < self.resubmits + 1:
                    jid = fails["jid"].item()
                    print "retrying " + str(jid)
                    self.qsub(runfile=self.jobs[jid]["runfile"],
                              jobname=self.jobs[jid]["jobname"],
                              jid=jid,
                              parameters=self.jobs[jid]["parameters"],
                              *self.jobs[jid]["args"],
                              **self.jobs[jid]["kwargs"])
            elif current_status == 5:
                del(self.jobs[jid])
            else:
                warnings.warn(("sge job {id} left the sge queue after "
                               "after registering an unknown status "
                               ).format(id=sgeid))
