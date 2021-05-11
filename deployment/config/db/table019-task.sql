--
-- Table structure for table `task`
--
use `docker`;

DROP TABLE IF EXISTS `task`;

CREATE TABLE `task` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `workflow_id` INTEGER DEFAULT NULL,
  `node_id` INTEGER DEFAULT NULL,
  `task_args_hash` varchar(150) NOT NULL,
  `name` varchar(250) DEFAULT NULL,
  `command` text,
  `executor_parameter_set_id` INTEGER DEFAULT NULL,
  `num_attempts` INTEGER DEFAULT NULL,
  `max_attempts` INTEGER DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;


ALTER TABLE `task` ADD INDEX `ix_workflow_id` (`workflow_id`);
ALTER TABLE `task` ADD INDEX `ix_node_id` (`node_id`);
ALTER TABLE `task` ADD INDEX `ix_task_args_hash` (`task_args_hash`);
ALTER TABLE `task` ADD INDEX `ix_executor_parameter_set_id` (`executor_parameter_set_id`);
ALTER TABLE `task` ADD INDEX `ix_status` (`status`);
ALTER TABLE `task` ADD INDEX `ix_status_date` (`status_date`);
ALTER TABLE `task` ADD INDEX `ix_workflow_id_status_date` (`workflow_id`,`status_date`);
