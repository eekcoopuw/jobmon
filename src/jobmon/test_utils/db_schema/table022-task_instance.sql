--
-- Table structure for table `task_instance`
--
use `docker`;

DROP TABLE IF EXISTS `task_instance`;
CREATE TABLE `task_instance`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `workflow_run_id` INTEGER DEFAULT NULL,
  `cluster_type_id` INTEGER DEFAULT NULL,
  `distributor_id` INTEGER DEFAULT NULL,
  `task_id` INTEGER NOT NULL,
  `task_resources_id` INTEGER DEFAULT NULL,
  `nodename` varchar(150) DEFAULT NULL,
  `process_group_id` INTEGER DEFAULT NULL,
  `usage_str` varchar(250) DEFAULT NULL,
  `wallclock` varchar(50) DEFAULT NULL,
  `maxrss` varchar(50) DEFAULT NULL,
  `maxpss` varchar(50) DEFAULT NULL,
  `cpu` varchar(50) DEFAULT NULL,
  `io` varchar(50) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `report_by_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `task_instance` ADD INDEX `ix_workflow_run_id` (`workflow_run_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_distributor_id` (`distributor_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_task_id` (`task_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_task_resources_id` (`task_resources_id`);
ALTER TABLE `task_instance` ADD INDEX `ix_status` (`status`);
ALTER TABLE `task_instance` ADD INDEX `ix_cluster_type_id` (`cluster_type_id`);
