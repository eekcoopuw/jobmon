--
-- Table structure for table `node_instance`
--

DROP TABLE IF EXISTS `node_instance`;

CREATE TABLE `node_instance` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `command_id` INTEGER DEFAULT NULL,
  `node_id` INTEGER DEFAULT NULL,
  `run_args_hash` varchar(150) NOT NULL,
  `status` varchar(1) NOT NULL,
  `status_date` datetime DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

-- add index instead of foreign key
ALTER TABLE `node_instance` ADD INDEX `ix_command_id` (`command_id`);
ALTER TABLE `node_instance` ADD INDEX `ix_node_id` (`node_id`);
ALTER TABLE `node_instance` ADD INDEX `ix_run_args_hash` (`run_args_hash`);
ALTER TABLE `node_instance` ADD INDEX `ix_status` (`status`);
ALTER TABLE `node_instance` ADD INDEX `ix_status_date` (`status_date`);

-- add unique constraint
ALTER TABLE `node_instance` ADD CONSTRAINT `uc_command_node_run` UNIQUE (`command_id`, `node_id`, `run_args_hash`);
