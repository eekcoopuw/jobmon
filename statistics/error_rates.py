import logging
import os

import pandas as pd
from db_tools import ezfuncs
from db_tools.config import DBConfig

logger = logging.getLogger(__name__)


def define_database_connections():
    jobmon_p01 = 'jobmon-p01.ihme.washington.edu'
    jobmon_docker_cont_p01 = 'jobmon-docker-cont-p01.hosts.ihme.washington.edu'
    dev_tomflem = 'dev-tomflem.ihme.washington.edu'

    db_config = DBConfig(load_base_defs=False, load_odbc_defs=False)

    # TBD Prompt users for passwords, or otherwise do this securely
    db_config._update_single_conn_def(
        {
            # 3305 has the annual burdenator run from GBD 2017
            "v063a": {
                "host": dev_tomflem,
                "port": 3305,
                "user_name": "docker",
                "password": "****"
            },
            "jobmon-nov2018": {
                "host": jobmon_p01,
                "port": 3309,  # guessed
                "user_name": "read_only",
                "password": "******"
            },
            "v060again": {
                "host": jobmon_p01,
                "port": 3311,
                "user_name": "read_only",
                "password": "*****",
                "default_schema": "docker"
            },
            "v061": {
                "host": dev_tomflem,
                "port": 3312,
                "user_name": "read_only",
                "password": "*****",
                "default_schema": "docker"
            },
            "v066": {
                "host": jobmon_p01,
                "port": 3313,
                "user_name": "read_only",
                "password": "*****",
                "default_schema": "docker"
            },
            "v067": {
                "host": jobmon_p01,
                "port": 3314,
                "user_name": "root",
                "password": "*****",
                "default_schema": "docker"
            },
            "v071": {
                "host": jobmon_p01,
                "port": 3316,
                "user_name": "read_only",
                "password": "*****",
                "default_schema": "docker"
            },
            "v072": {
                "host": jobmon_p01,
                "port": 3317,
                "user_name": "read_only",
                "password": "*****",
                "default_schema": "docker"
            },
            "v080": {
                "host": jobmon_p01,
                "port": 3800,
                "user_name": "read_only",
                "password": "****"
            },
            "v081": {
                "host": jobmon_p01,
                "port": 3810,
                "user_name": "read_only",
                "password": "*****"
            },
            "v083": {
                "host": jobmon_p01,
                "port": 3830,
                "user_name": "read_only",
                "password": "*****"
            },
            "v089": {
                "host": jobmon_docker_cont_p01,
                "port": 3890,
                "user_name": "read_only",
                "password": "*****"
            },
            "v095": {
                "host": jobmon_docker_cont_p01,
                "port": 3950,
                "user_name": "read_only",
                "password": "*****"
            },
            "v099": {
                "host": jobmon_p01,
                "port": 3990,
                "user_name": "read_only",
                "password": "*****"
            },
            "v100": {
                "host": jobmon_docker_cont_p01,
                "port": 10000,
                "user_name": "read_only",
                "password": "*****"
            },
            "v101": {
                "host": jobmon_docker_cont_p01,
                "port": 10010,
                "user_name": "read_only",
                "password": "*****"
            }
        })
    return db_config


def database_conns_from_file(kpi_odbc):
    db_config = DBConfig(load_base_defs=False, load_odbc_defs=True,
                         odbc_filepath=kpi_odbc)
    return db_config


def get_workflow_statistics(conn_name: str) -> pd.DataFrame:
    """
     Number of Workflows by state and number of jobs
    :return:
    """
    query = """
    SELECT
      workflow.id, workflow.workflow_args, workflow.description,
      workflow.status, workflow.status_date,  
      COUNT(job.job_id) as number_of_jobs 
    FROM docker.workflow
    JOIN docker.job
    WHERE job.dag_id = workflow.dag_id 
    GROUP BY workflow.id
    """
    print(f"  Querying get_workflow_statistics: {conn_name}")
    df = ezfuncs.query(query, conn_def=conn_name)
    df['database'] = pd.Series(conn_name, index=df.index)
    return df


def get_job_statistics(conn_name: str) -> pd.DataFrame:
    """
     Number of Workflows by state and number of jobs.
     The number of successful retries can be found by
     status ='D' and num_attempts > 1
    :return:
    """
    query = """
    SELECT
      dag_id, job_id, status, status_date, 
      num_attempts, max_attempts
    FROM docker.job
    """
    print(f"  Querying get_job_statistics: {conn_name}")
    df = ezfuncs.query(query, conn_def=conn_name)
    df['database'] = pd.Series(conn_name, index=df.index)
    return df


def get_resume_statistics(conn_name: str) -> pd.DataFrame:
    """
     How many workflows were resumed
    :return:
    """
    query = """
SELECT
      W.id as w_id,
      WR.id as wr_id,
      W.status as w_status, 
      WR.status as wr_status,
      WR.status_date as wr_status_date
    FROM docker.workflow W
    JOIN docker.workflow_run WR 
    where W.id = WR.workflow_id
    """
    print(f"  Querying get_resume_statistics: {conn_name}")
    df = ezfuncs.query(query, conn_def=conn_name)
    df['database'] = pd.Series(conn_name, index=df.index)
    return df


def should_be_collected(conn_name: str) -> pd.DataFrame:
    """ For the speedup of job packing.
    Was not collected, but should have been. From job dbs 3306
    063again, used during the annual runs of the burdenator"""

    query = """
    select J.command, JI.cpu, JI.status from job_instance JI
    join job J on JI.job_id = J.`job_id`
    where J.dag_id in (938,939,940) and
    J.command like 'run_burdenator_most_detailed%' and JI.status='D'
    """
    print(f"  Querying job_packing on annual run: {conn_name}")
    df = ezfuncs.query(query, conn_def=conn_name)
    df['database'] = pd.Series(conn_name, index=df.index)
    return df


def main():
    """
    Get basic envelope statistics:
    Number of Workflows by state and number of jobs
    Jobs by language, and states
    Job retry rates
    Clusters usage: slots, hours

    Get them into pandas frames
    """

    # Megan created an odbc with all of the jobmon databases and passwords
    kpi_odbc = "/ihme/scratch/users/svcscicompci/kpi_odbc.ini"
    if os.path.exists(kpi_odbc):
        db_config = database_conns_from_file(kpi_odbc)
    else:
        db_config = define_database_connections()

    all_workflows = None
    all_jobs = None
    all_resumes = None

    # keeping for posterity - these were for q3-q4 of 2018(?)
    # q3_q4_dbs = ["v071", "v072", "v080", "v081", "v083", "v089"]

    # 07/17/2019 KPIs
    db_list = ["v095", "v099", "v100", "v101"]

    # There are cleverer ways but this gets the job done
    for name in db_list:
        print(f"Accessing {name}")
        w_df = get_workflow_statistics(name)
        j_df = get_job_statistics(name)
        r_df = get_resume_statistics(name)

        try:
            if not all_workflows.empty:
                all_workflows = all_workflows.append(w_df)
                all_jobs = all_jobs.append(j_df)
                all_resumes = all_resumes.append(r_df)
        except:
            all_workflows = w_df
            all_jobs = j_df
            all_resumes = r_df

    if not all_workflows.empty:
        all_workflows.to_hdf('stats_workflows.h5', key='counts')
        all_jobs.to_hdf('stats_jobs.h5', key='counts')
        all_resumes.to_hdf('stats_resumes.h5', key='counts')


if __name__ == "__main__":
    main()
