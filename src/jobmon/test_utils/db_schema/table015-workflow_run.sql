--
-- Table structure for table `workflow_run`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_run`;

CREATE TABLE `workflow_run` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `workflow_id` INTEGER DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `jobmon_version` varchar(150) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `heartbeat_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `workflow_run` ADD INDEX `ix_workflow_id` (`workflow_id`);
ALTER TABLE `workflow_run` ADD INDEX `ix_status_version` (`status`, `jobmon_version`);
