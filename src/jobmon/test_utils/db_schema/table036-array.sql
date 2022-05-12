--
-- Table structure for table `array`
--
use `docker`;

DROP TABLE IF EXISTS `array`;

CREATE TABLE `array` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_template_version_id` INTEGER,
  `workflow_id` INTEGER,
  `max_concurrently_running` INTEGER,
  `created_date` datetime DEFAULT NULL,
   PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `array` ADD INDEX `ix_task_template_version_id` (`task_template_version_id`);
ALTER TABLE `array` ADD INDEX `ix_workflow_id` (`workflow_id`);

-- add constraint on task_template_version_id/workflow_id
ALTER TABLE `array` ADD CONSTRAINT `uc_task_template_version_id_workflow_id` UNIQUE (`task_template_version_id`, `workflow_id`);

-- add name colum; see GBDSCI-4184
ALTER TABLE `array` ADD COLUMN `name` varchar(255) NOT NULL;
ALTER TABLE `array` ADD INDEX `ix_name` (`name`);
