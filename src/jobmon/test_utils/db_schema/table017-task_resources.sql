--
-- Table structure for table `task_resources`
--
use `docker`;

DROP TABLE IF EXISTS `task_resources`;
CREATE TABLE `task_resources` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `queue_id` INTEGER NULL,
  `task_resources_type_id` varchar(1) NOT NULL,
  `requested_resources` TEXT DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `task_resources` ADD INDEX `ix_queue_id` (`queue_id`);
ALTER TABLE `task_resources` ADD INDEX `ix_task_resources_type_id` (`task_resources_type_id`);
