create index 'job_instance_executor_id'
ON job_instance(executor_id) COMMENT 'SGE-ids can be reused, so this is not unique';