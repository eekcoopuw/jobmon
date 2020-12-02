USE docker;


DROP PROCEDURE IF EXISTS split_future_partition;

DELIMITER $$
CREATE PROCEDURE split_future_partition(p_schema varchar(64), p_table varchar(64), p_name varchar(64), p_max_value int)
   LANGUAGE SQL
   NOT DETERMINISTIC
   SQL SECURITY INVOKER
BEGIN  

   -- construct the reorganize statement
   SELECT CONCAT(
      'ALTER TABLE ', @p_schema, '.', @p_table,
      ' REORGANIZE PARTITION future INTO (',
      'PARTITION ', @p_name, ' VALUES LESS THAN (', @p_max_value, '), '
      'PARTITION future VALUES LESS THAN (MAXVALUE))'
   );

   -- And then we prepare and execute the ALTER TABLE query.
   PREPARE st FROM @query;
   EXECUTE st;
   DEALLOCATE PREPARE st;  

END$$
DELIMITER ;



DROP PROCEDURE IF EXISTS repartition;

DELIMITER $$
CREATE PROCEDURE repartition(p_schema varchar(64), p_name varchar(64))
   LANGUAGE SQL
   NOT DETERMINISTIC
   SQL SECURITY INVOKER
BEGIN  

   -- workflow partitioning
   SELECT MAX(id) FROM workflow INTO @p_max_value;
   CALL split_future_partition(@p_schema, 'workflow', @p_name, @p_max_value);
      
   -- workflow_run partitioning
   SELECT MAX(id) FROM workflow_run INTO @p_max_value;
   CALL split_future_partition(@p_schema, 'workflow_run', @p_name, @p_max_value);

   -- executor_parameter_set partitioning
   SELECT MAX(id) FROM executor_parameter_set INTO @p_max_value;
   CALL split_future_partition(@p_schema, 'executor_parameter_set', @p_name, @p_max_value);

   -- task partitioning
   SELECT MAX(id) FROM task INTO @p_max_value;
   CALL split_future_partition(@p_schema, 'task', @p_name, @p_max_value);

   -- task_instance partitioning
   SELECT MAX(id) FROM task_instance INTO @p_max_value;
   CALL split_future_partition(@p_schema, 'task_instance', @p_name, @p_max_value);

   -- task_instance_error_log partitioning
   SELECT MAX(id) FROM task_instance_error_log INTO @p_max_value;
   CALL split_future_partition(@p_schema, 'task_instance_error_log', @p_name, @p_max_value);


END$$
DELIMITER ;
