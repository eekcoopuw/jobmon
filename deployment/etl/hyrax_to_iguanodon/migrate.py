import os
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import math
import ast
import json

SOURCE_DB_HOST = "scicomp-maria-db-d01.db.ihme.washington.edu" # "scicomp-maria-db-p02.db.ihme.washington.edu"
SOURCE_DB_PORT = 3306
SOURCE_DB_INSTANCE_IDENTIFIER = "docker_prod_1022"
SOURCE_DB_USER = "read_only"

TARGET_DB_HOST = "scicomp-maria-db-d01.db.ihme.washington.edu"
TARGET_DB_PORT = 3306
TARGET_DB_INSTANCE_IDENTIFIER = "zshort_docker"
TARGET_DB_USER = "root"

LOG_FILE = None

table_list = [

    # passable small section
    'arg',
    'dag',

    # large size
    'edge',
    'node',
    'node_arg',
    'task',
    'task_arg',

    # passable small section
    'task_attribute',
    'task_attribute_type',

    # large size
    'task_instance',
    'task_instance_error_log',

    # passable small section
    'task_template',
    'task_template_version',
    'template_arg_map',
    'tool',
    'tool_version',
    'workflow',
    'workflow_attribute',
    'workflow_attribute_type',
    'workflow_run',

    # complex piece
    'executor_parameter_set',

    # NO NEED TO migrate: 'arg_type',
    # NO NEED TO migrate: 'task_instance_status',
    # NO NEED TO migrate: 'task_status',
    # NO NEED TO migrate: 'workflow_run_status',
    # NO NEED TO migrate: 'workflow_status',
]

def requested_resources(max_runtime_seconds: int, context_args: str, num_cores: int,
                        m_mem_free: float, j_resource: int, hard_limits: int) -> str:
    rr_dict = {}

    if max_runtime_seconds is not None and math.isnan(max_runtime_seconds) == False:
        rr_dict["runtime"] = max_runtime_seconds
    if num_cores is not None and math.isnan(num_cores) == False:
        rr_dict["cores"] = num_cores
    if m_mem_free is not None and math.isnan(m_mem_free) == False:
        rr_dict["memory"] = m_mem_free
    if j_resource > 0:
        rr_dict["constraints"] = "archive"
    if context_args is not None and context_args != "{}":
        ca = ast.literal_eval(context_args)
        rr_dict.update(ca)

    return json.dumps(rr_dict)

def transform_resource_scales(orig_resource_scales: str) -> str:
    """
    Input param:
        orig_resource_scales - map string with keys: 'm_mem_free', 'max_runtime_seconds'

    Returns:
        return with new keys: memory, runtime
    """
    if orig_resource_scales is None:
        return None

    new_resource_scales = ast.literal_eval(orig_resource_scales)
    m_mem_free_val = new_resource_scales["m_mem_free"]
    max_runtime_seconds_val = new_resource_scales["max_runtime_seconds"]
    if m_mem_free_val is not None:
        new_resource_scales["memory"] = new_resource_scales["m_mem_free"]
        del new_resource_scales["m_mem_free"]
    if max_runtime_seconds_val is not None:
        new_resource_scales["runtime"] = new_resource_scales["max_runtime_seconds"]
        del new_resource_scales["max_runtime_seconds"]

    return json.dumps(new_resource_scales)

def migrate():
    print("\n*********** Loading data from SOURCE DB ***********\n")
    print("Connecting to [" + SOURCE_DB_HOST + "] ...")
    SOURCE_DB_PASSWORD = input(f"Enter your SOURCE PASS for {SOURCE_DB_USER}: ")
    source_engine = create_engine(
        'mysql://' + SOURCE_DB_USER + ':' + SOURCE_DB_PASSWORD + '@' +
        SOURCE_DB_HOST + '/' + SOURCE_DB_INSTANCE_IDENTIFIER + '?charset=utf8mb4')
    # Activate server side cursor
    source_conn = source_engine.connect().execution_options(stream_results=True)

    print("Connecting to [" + TARGET_DB_HOST + "] ...")
    TARGET_DB_PASSWORD = input(f"Enter your TARGET PASS for {TARGET_DB_USER}: ")
    target_engine = create_engine(
        'mysql://' + TARGET_DB_USER + ':' + TARGET_DB_PASSWORD + '@' +
        TARGET_DB_HOST + '/' + TARGET_DB_INSTANCE_IDENTIFIER + '?charset=utf8mb4')
    target_conn = target_engine.connect().execution_options(stream_results=False)

    df_cluster = pd.read_sql("select * from cluster", target_conn)
    print(f"df_cluster = \n{df_cluster}")

    df_cluster_type = pd.read_sql("select * from cluster_type", target_conn)
    # Transform UGE to sge in preparation for comparison.
    df_cluster_type.loc[df_cluster_type['name'].str.lower().str.startswith('uge'), 'name'] = "sge"
    df_cluster_type['name'] = df_cluster_type['name'].astype(str) + "executor"
    print(f"df_cluster_type = \n{df_cluster_type}")

    df_task_resources_type = pd.read_sql("select * from task_resources_type", target_conn)
    print(f"df_task_resources_type = \n{df_task_resources_type}")

    sql_queue = "select q.id as queue_id, q.name as queue, ct.name as cluster_type_name " \
                "from queue q " \
                "inner join cluster c on q.cluster_id = c.id " \
                "inner join cluster_type ct on c.cluster_type_id = ct.id"
    df_queue = pd.read_sql(sql_queue, target_conn)
    df_queue.loc[df_queue['cluster_type_name'].str.lower().str.startswith('uge'), 'cluster_type_name'] = "sge"
    df_queue['cluster_type_name'] = df_queue['cluster_type_name'].astype(str) + "executor"
    print(f"df_queue = \n{df_queue}")

    for t in table_list:

        print(f"Fetching data to DataFrame chunk by chunk ... {t}")

        if t == "workflow_run":
            select_str = "id, workflow_id, user, jobmon_version, status, created_date, status_date, heartbeat_date"
        elif t == "task":
            select_str = \
                "t.id, t.workflow_id, t.node_id, t.task_args_hash, t.name, t.command, " \
                "t.executor_parameter_set_id as task_resources_id, t.num_attempts, " \
                "t.max_attempts, eps.resource_scales as resource_scales, " \
                "NULL as fallback_queues, t.status, t.submitted_date, t.status_date"
        elif t == "task_instance":
            select_str = "id, workflow_run_id, " \
                         "executor_type, " \
                         "executor_id as distributor_id, task_id, " \
                         "executor_parameter_set_id as task_resources_id, nodename, " \
                         "process_group_id, usage_str, wallclock, " \
                         "maxrss, maxpss, cpu, io, status, submitted_date, status_date, report_by_date"
        elif t == "executor_parameter_set":
            select_str = "e.id, e.task_id, e.queue, e.parameter_set_type as task_resources_type_id," \
                         "lower(wfr.executor_class) as cluster_type_name, " \
                         "e.max_runtime_seconds, e.context_args, e.num_cores, e.m_mem_free, " \
                         "e.j_resource, e.hard_limits"
        else:
            select_str = "*"

        if t == "task":
            from_str = "task t " \
                       "left outer join " \
                       "(select task_id, max(resource_scales) as resource_scales " \
                       "from executor_parameter_set " \
                       "group by task_id) eps on t.id = eps.task_id"
        elif t == "executor_parameter_set":
            from_str = "executor_parameter_set e " \
                       "join task t on e.task_id = t.id " \
                       "join workflow w on t.workflow_id = w.id " \
                       "join " \
                       "(select workflow_id, max(executor_class) as executor_class, " \
                       "count(distinct executor_class) as distcnt " \
                       "from workflow_run " \
                       "group by workflow_id) wfr " \
                       "on w.id = wfr.workflow_id"
        else:
            from_str = t

        if t == "tool":
            where_str = " where id <> 1"
        elif t == "tool_version":
            where_str = " where id <> 1"
        else:
            where_str = ""

        sql = f"select {select_str} from {from_str}{where_str}"

        with open(LOG_FILE, "a") as file_object:
            file_object.write(f"{t} - {str(datetime.now())}\n")
            file_object.write(f"sql = \n{sql}\n")

        cnt = 0

        for chunk_df in pd.read_sql(sql, source_conn, chunksize=10_000):
            print(f"chunk_df = \n{chunk_df}")
            with open(LOG_FILE, "a") as file_object:
                file_object.write(f"chunk_df = \n{chunk_df}\n\n")
            if t == "task":
                chunk_df['resource_scales'] = \
                    chunk_df.apply(lambda x: transform_resource_scales(x['resource_scales']), axis=1)
                print(f"chunk_df AFTER TRANSFORM = \n{chunk_df}")
                with open(LOG_FILE, "a") as file_object:
                    file_object.write(f"chunk_df AFTER TRANSFORM = \n{chunk_df}\n\n")
            elif t == "task_instance":
                chunk_df['cluster_type_id'] = \
                    chunk_df['executor_type'].str.lower().map(df_cluster_type.set_index('name')['id'])
                del chunk_df['executor_type']
                col = chunk_df.pop('cluster_type_id')
                chunk_df.insert(2, col.name, col)
                print(f"chunk_df AFTER TRANSFORM = \n{chunk_df}")
                with open(LOG_FILE, "a") as file_object:
                    file_object.write(f"chunk_df AFTER TRANSFORM = \n{chunk_df}\n\n")
            elif t == "executor_parameter_set":
                chunk_df = pd.merge(chunk_df, df_queue, on=["queue", "cluster_type_name"], how="left")
                del chunk_df['queue']
                del chunk_df['cluster_type_name']
                chunk_df['requested_resources'] = \
                    chunk_df.apply(lambda x:
                                   requested_resources(x["max_runtime_seconds"], x["context_args"],
                                        x["num_cores"], x["m_mem_free"], x["j_resource"],
                                        x["hard_limits"]), axis=1)
                del chunk_df['max_runtime_seconds']
                del chunk_df['context_args']
                del chunk_df['num_cores']
                del chunk_df['m_mem_free']
                del chunk_df['j_resource']
                del chunk_df['hard_limits']
                print(f"chunk_df AFTER TRANSFORM = \n{chunk_df}")
                with open(LOG_FILE, "a") as file_object:
                    file_object.write(f"chunk_df AFTER TRANSFORM = \n{chunk_df}\n\n")

            to_t = "task_resources" if t == "executor_parameter_set" else t

            print(f"Migrating data ... {to_t}")

            chunk_df.to_sql(to_t, target_engine, if_exists='append', index=False, method='multi')

            # limit to 10_000 for quick test
            if t in [
                'edge',
                'node',
                'node_arg',
                'task',
                'task_arg',
                'task_instance',
                'task_instance_error_log',
                'executor_parameter_set',
                ]:
                break

            # cnt+= 1
            #
            # if cnt >= 10:
            #     break

    print("Closing connection - Source ...")
    source_conn.close()
    source_engine.dispose()

    print("Closing connection - Target ...")
    target_conn.close()
    target_engine.dispose()
    print("\n***********      Migration done   ***********\n")


if __name__ == '__main__':

    dateTimeObj_before = datetime.now()

    f = open(f"tables_loading_{dateTimeObj_before.strftime('%Y_%m_%d_%H_%M_%S')}.log", 'a+')
    LOG_FILE = f.name
    f.close()

    migrate()

    dateTimeObj_after = datetime.now()

    print(f"Time before migration: {dateTimeObj_before}")
    with open(LOG_FILE, "a") as file_object:
        file_object.write(f"Time before migration: {dateTimeObj_before}\n")
    print(f"Time after migration: {dateTimeObj_after}")
    with open(LOG_FILE, "a") as file_object:
        file_object.write(f"Time after migration: {dateTimeObj_after}\n")

    print(f"Time spent: {dateTimeObj_after - dateTimeObj_before}")
    with open(LOG_FILE, "a") as file_object:
        file_object.write(f"Time spent: {dateTimeObj_after - dateTimeObj_before}\n")
