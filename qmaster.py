import os
import time
import subprocess
import warnings
from datetime import datetime
from . import sge, job


this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))


class IgnorantQ(object):

    def __init__(self):
        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}

    def qsub(self, *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub. see sge module for
        documentation

        Returns:
            sge job id
        """
        sgeid = sge.qsub(*args, **kwargs)
        self.scheduled_jobs.append(sgeid)
        self.jobs[sgeid] = {"args": args, "kwargs": kwargs}
        return sgeid

    def qblock(self, poll_interval=10):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue. check sge queue each poll_interval until q is clear
        run qmanage each poll_interval.

        Args:
            poll_interval (int, optional): time in seconds between each qmanage
            call

        """
        while not self.qcomplete():
            time.sleep(poll_interval)

    def qmanage(self):
        """run """
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
        return len(self.scheduled_jobs == 0)

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


class MonitorState(object):

    def __init__(self):
        self.monitor = None
        self.i = 0
        self.status = "stopped"

    @property
    def i(self):
        """auto increment for job id"""
        i = self._i
        self._i = self._i + 1
        return i

    @i.setter
    def i(self, value):
        self._i = value


class MonitoredQ(IgnorantQ):

    monitors = {}

    def __init__(self, out_dir, retries=0):
        self.out_dir = out_dir
        self.retries = retries

        # internal tracking
        self.scheduled_jobs = []
        self.jobs = {}

        # internal server and client
        self.start_monitor(out_dir)
        time.sleep(5)  # this solution is unsatisfying
        self.manager = job.ManageJobMonitor(out_dir)

    @classmethod
    def get_monitor_state(cls, out_dir):
        """return incremented auto incremented i value. This value will be
        unique within a python instance"""
        try:
            mon_state = cls.monitors[out_dir]
        except KeyError:
            cls.monitors[out_dir] = MonitorState()
            mon_state = cls.monitors[out_dir]
        return mon_state

    @classmethod
    def set_monitor_state(cls, out_dir, mon_state):
        cls.monitors[out_dir] = mon_state

    @property
    def i(self):
        mon_state = self.get_monitor_state(self.out_dir)
        return mon_state.i

    def start_monitor(self, out_dir):
        """start a jobmonitor server in a subprocess. MonitoredQ's share
        monitor servers and auto increments if they are initialized with the
        same out_dir"""
        mon_state = self.get_monitor_state(self.out_dir)

        if mon_state.status == "stopped":
            mon_state.monitor = subprocess.Popen(
                ['/ihme/code/central_comp/anaconda/envs/py35/bin/python',
                 this_dir + '/bin/launch_monitor.py',
                 out_dir])
            mon_state.status = "running"

    def stop_monitor(self):
        """stop jobmonitor server tied to this instance"""
        mon_state = self.get_monitor_state(self.out_dir)
        if mon_state.status == "running":
            mon_state.monitor = None
            mon_state.status = "stopped"
            self.manager.stop_server()

    def qsub(self, runfile, jobname, jid=None, parameters=[],
             *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub.

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
            jid = self.i
        base_params = ["--mon_dir", self.out_dir, "--runfile", runfile,
                       "--jid", jid]

        # replace -- with ## to allow for passthrough in monitored job
        passed_params = []
        for param in parameters:
            passed_params.append(str(param).replace("--", "##", 1))

        # append additional parameters
        if parameters:
            parameters = base_params + passed_params
        else:
            parameters = base_params

        # submit. store submission params in self.jobs dict in case resubmit
        sgeid = sge.qsub(runfile=this_dir + "/bin/monitored_job.py",
                         jobname=jobname, parameters=parameters, *args,
                         **kwargs)
        self.scheduled_jobs.append(sgeid)
        self.jobs[jid] = {"runfile": runfile,
                          "jobname": jobname,
                          "parameters": parameters,
                          "args": args,
                          "kwargs": kwargs}
        return sgeid

    def manage_exit_q(self, exit_jobs):

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
                query_status + "sgeid = {sgeid};".format(sgeid=sgeid)
                )[1]
            current_status = result["current_status"].item()
            jid = result["jid"].item()

            if current_status == 1:
                warnings.warn(("sge job {id} left the sge queue without "
                               "starting job execution. this is probably "
                               "bad.").format(id=sgeid))
            elif current_status == 2:
                warnings.warn(("sge job {id} left the sge queue after "
                               "starting job execution. but did not "
                               "register an error and did not register "
                               "completed. This is probably bad."
                               ).format(id=sgeid))
            elif current_status == 3:
                fails = self.manager.query(
                    query_failed + " AND sgeid = {id};".format(id=sgeid)
                    )[1]
                if fails["num"].item() < self.retries + 1:
                    jid = fails["jid"].item()
                    print "retrying " + str(jid)
                    self.qsub(runfile=self.jobs[jid]["runfile"],
                              jobname=self.jobs[jid]["jobname"],
                              jid=jid,
                              parameters=self.jobs[jid]["parameters"],
                              *self.jobs[jid]["args"],
                              **self.jobs[jid]["kwargs"])
            elif current_status == 4:
                del(self.jobs[jid])
            else:
                warnings.warn(("sge job {id} left the sge queue after "
                               "after registering an unknown status "
                               ).format(id=sgeid))
