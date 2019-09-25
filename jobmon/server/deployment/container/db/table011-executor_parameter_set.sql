--
-- Table structure for table `executor_parameter_set`
--
use `docker`;

DROP TABLE IF EXISTS `executor_parameter_set`;
CREATE TABLE `executor_parameter_set` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) NOT NULL,
  `parameter_set_type` varchar(1) NOT NULL,
  `max_runtime_seconds` int(11) DEFAULT NULL,
  `context_args` varchar(1000) DEFAULT NULL,
  `resource_scales` varchar(1000) DEFAULT NULL,
  `queue` varchar(255) DEFAULT NULL,
  `num_cores` int(11) DEFAULT NULL,
  `m_mem_free` float DEFAULT NULL,
  `j_resource` tinyint(1) DEFAULT NULL,
  `hard_limits` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `parameter_set_type` (`parameter_set_type`),
  KEY `job_id` (`job_id`),
  CONSTRAINT `executor_parameter_set_ibfk_1` FOREIGN KEY (`parameter_set_type`) REFERENCES `executor_parameter_set_type` (`id`),
  CONSTRAINT `executor_parameter_set_ibfk_2` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1;
