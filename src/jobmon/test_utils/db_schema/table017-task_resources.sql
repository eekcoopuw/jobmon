--
-- Table structure for table `task_resources`
--
use `docker`;

DROP TABLE IF EXISTS `task_resources`;
CREATE TABLE `task_resources` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_id` INTEGER NOT NULL,
  `queue_id` INTEGER NULL,
  `resources_type` varchar(1) NOT NULL,
  `resource_scales` varchar(1000) DEFAULT NULL,
  `requested_resources` TEXT DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `task_resources` ADD INDEX `ix_task_id` (`task_id`);
ALTER TABLE `task_resources` ADD INDEX `ix_queue_id` (`queue_id`);
ALTER TABLE `task_resources` ADD INDEX `ix_resources_type` (`resources_type`);
