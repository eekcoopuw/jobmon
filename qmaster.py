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
        self.jobs = {}

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

    def qmonitor(self):
        """wait until all jobs submitted through this qmaster instance have
        left the sge queue. resubmit each job 'retries' times if it fails.
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
            for sgeid in missing_jobs:
                # remove from sge tracker
                self.scheduled_jobs.remove(sgeid)
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
