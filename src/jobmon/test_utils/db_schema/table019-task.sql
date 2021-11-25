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
  `array_id` INTEGER DEFAULT NULL,
  `name` varchar(250) DEFAULT NULL,
  `command` text,
  `task_resources_id` INTEGER DEFAULT NULL,
  `num_attempts` INTEGER DEFAULT NULL,
  `max_attempts` INTEGER DEFAULT NULL,
  `resource_scales` varchar(1000) DEFAULT NULL,
  `fallback_queues` varchar(1000) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;


ALTER TABLE `task` ADD INDEX `ix_workflow_id` (`workflow_id`);
ALTER TABLE `task` ADD INDEX `ix_node_id` (`node_id`);
ALTER TABLE `task` ADD INDEX `ix_array_id` (`array_id`);
ALTER TABLE `task` ADD INDEX `ix_task_args_hash` (`task_args_hash`);
ALTER TABLE `task` ADD INDEX `ix_task_resources_id` (`task_resources_id`);
ALTER TABLE `task` ADD INDEX `ix_status` (`status`);
ALTER TABLE `task` ADD INDEX `ix_status_date` (`status_date`);
ALTER TABLE `task` ADD INDEX `ix_workflow_id_status_date` (`workflow_id`,`status_date`);
