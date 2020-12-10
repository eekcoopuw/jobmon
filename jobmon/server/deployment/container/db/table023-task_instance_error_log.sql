--
-- Table structure for table `task_instance_error_log`
--
use `docker`;

DROP TABLE IF EXISTS `task_instance_error_log`;

CREATE TABLE `task_instance_error_log` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_instance_id` INTEGER NOT NULL,
  `error_time` datetime DEFAULT NULL,
  `description` text,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

ALTER TABLE `task_instance_error_log` ADD INDEX `ix_task_instance_id` (`task_instance_id`);
