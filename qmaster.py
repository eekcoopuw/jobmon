import os
import time
import subprocess
import warnings
import pandas as pd
from datetime import datetime
from . import sge
from . import job


this_file = os.path.abspath(os.path.expanduser(__file__))
this_dir = os.path.dirname(os.path.realpath(this_file))


class IgnorantQ(object):

    def __init__(self, poll_interval=10):
        self.scheduled_jobs = []
        self.poll_interval = poll_interval

    def qsub(self, *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub"""
        jid = sge.qsub(*args, **kwargs)
        self.scheduled_jobs.append(jid)
        return jid

    def qmonitor(self):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue.
        """
        while len(self.scheduled_jobs) > 0:
            time.sleep(self.poll_interval)
            print 'Polling jobs ... {}'.format(datetime.now())
            current_jobs = set(sge.qstat(jids=self.scheduled_jobs
                                         ).job_id.tolist())
            print '             ... ' + str(len(current_jobs)) + ' active jobs'
            scheduled_jobs = set(self.scheduled_jobs)
            missing_jobs = scheduled_jobs - current_jobs
            for jid in missing_jobs:
                self.scheduled_jobs.remove(jid)


class MonitoredQ(object):

    i = 0

    def __init__(self, out_dir, poll_interval=10, retries=0):
        self.out_dir = out_dir
        self.poll_interval = poll_interval
        self.retries = retries

        # internal variables
        self.scheduled_jobs = []
        self.jobid_monid = pd.DataFrame(columns=["monid", "jid"])

        # internal objects
        self.monitor = self.start_monitor(out_dir)
        time.sleep(5)
        self.manager = ManageJobMonitor(out_dir)

    @staticmethod
    def start_monitor(dir_path):
        '''start a jobmonitor server in a subprocess '''
        monitor = subprocess.Popen(
            ['/ihme/code/central_comp/anaconda/envs/py35/bin/python',
             this_dir + '/bin/launch_monitor.py',
             dir_path])
        return monitor

    def stop_monitor(self):
        self.manager.stop_server()

    def qsub(self, runfile, jobname, jid=None, parameters=[],
             *args, **kwargs):
        """submit jobs to sge scheduler using sge.qsub"""

        # configure arguments for parsing by ./bin/monitored_job.py
        if jid is None:
            jid = self.i
            self.i = self.i + 1
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

        # submit
        jid = sge.qsub(runfile=this_dir + "/bin/monitored_job.py",
                       jobname=jobname, parameters=parameters, *args, **kwargs)
        self.scheduled_jobs.append(jid)

        # # add to internal tracker
        # self.jobid_monid.loc[len(self.jobid_monid)] = [jid, self.i]
        # self.i = self.i + 1
        return jid

    def qmonitor(self):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue. resubmit each job 'retries' times if it fails.
        """
        q_num_succeed = "SELECT count(*) FROM job_status WHERE status = 4"
        q_num_failed = "SELECT count(*) FROM job_status WHERE status = 3"

        while len(self.scheduled_jobs) > 0:

            # sleepy time
            time.sleep(self.poll_interval)
            print 'Polling jobs ... {}'.format(datetime.now())

            # find jobs that have left the sge queue
            current_jobs = set(sge.qstat(jids=self.scheduled_jobs
                                         ).job_id.tolist())
            print '             ... ' + str(len(current_jobs)) + ' active jobs'
            scheduled_jobs = set(self.scheduled_jobs)
            missing_jobs = scheduled_jobs - current_jobs

            # find the database ids for

            # loop through jobs that have left the sge queue
            for jid in missing_jobs:
                self.scheduled_jobs.remove(jid)


class ManageJobMonitor(job.Manager):

    def query(self, query):
        """execute query on sqlite database through server
        Args:
            query (string): raw sql query string to execute on sqlite monitor
                database
        """
        msg = {'action': 'query', 'args': [query]}
        response = self.send_request(msg)
        return response
