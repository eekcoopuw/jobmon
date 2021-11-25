--
-- Table structure for table `array`
--
use `docker`;

DROP TABLE IF EXISTS `array`;

CREATE TABLE `array` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_template_version_id` INTEGER,
  `workflow_id` INTEGER,
  `task_resources_id` INTEGER,
  `max_concurrently_running` INTEGER,
  `threshold_to_submit` INTEGER,
  `num_completed` INTEGER DEFAULT NULL,
  `cluster_id` INTEGER,
  `created_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `array` ADD INDEX `ix_task_template_version_id` (`task_template_version_id`);
ALTER TABLE `array` ADD INDEX `ix_workflow_id` (`workflow_id`);
ALTER TABLE `array` ADD INDEX `ix_task_resources_id` (`task_resources_id`);

-- add constraint on task_template_version_id/workflow_id
ALTER TABLE `array` ADD CONSTRAINT `uc_task_template_version_id_workflow_id` UNIQUE (`task_template_version_id`, `workflow_id`);
