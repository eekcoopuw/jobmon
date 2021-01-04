--
-- Table structure for table `workflow`
--
use `docker`;

DROP TABLE IF EXISTS `workflow`;

CREATE TABLE `workflow` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `tool_version_id` INTEGER DEFAULT NULL,
  `dag_id` INTEGER DEFAULT NULL,
  `workflow_args_hash` varchar(150),
  `task_hash` varchar(150),
  `description` text DEFAULT NULL,
  `name` varchar(150) DEFAULT NULL,
  `workflow_args` text DEFAULT NULL,
  `max_concurrently_running` INTEGER DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

ALTER TABLE `workflow` ADD INDEX `ix_tool_version_id` (`tool_version_id`);
ALTER TABLE `workflow` ADD INDEX `ix_dag_id` (`dag_id`);
ALTER TABLE `workflow` ADD INDEX `ix_workflow_args_hash` (`workflow_args_hash`);
ALTER TABLE `workflow` ADD INDEX `ix_task_hash` (`task_hash`);
ALTER TABLE `workflow` ADD INDEX `ix_status` (`status`);
