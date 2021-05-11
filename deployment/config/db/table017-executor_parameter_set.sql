--
-- Table structure for table `executor_parameter_set`
--
use `docker`;

DROP TABLE IF EXISTS `executor_parameter_set`;
CREATE TABLE `executor_parameter_set` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_id` INTEGER NOT NULL,
  `parameter_set_type` varchar(1) NOT NULL,
  `max_runtime_seconds` INTEGER DEFAULT NULL,
  `context_args` varchar(1000) DEFAULT NULL,
  `resource_scales` varchar(1000) DEFAULT NULL,
  `queue` varchar(255) DEFAULT NULL,
  `num_cores` INTEGER DEFAULT NULL,
  `m_mem_free` float DEFAULT NULL,
  `j_resource` tinyint(1) DEFAULT NULL,
  `hard_limits` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `executor_parameter_set` ADD INDEX `ix_task_id` (`task_id`);
ALTER TABLE `executor_parameter_set` ADD INDEX `ix_parameter_set_type` (`parameter_set_type`);
