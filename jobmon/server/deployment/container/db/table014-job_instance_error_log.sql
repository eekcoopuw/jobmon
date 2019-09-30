--
-- Table structure for table `job_instance_error_log`
--
use `docker`;

DROP TABLE IF EXISTS `job_instance_error_log`;
CREATE TABLE `job_instance_error_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_instance_id` int(11) NOT NULL,
  `error_time` datetime DEFAULT NULL,
  `description` text,
  PRIMARY KEY (`id`),
  KEY `job_instance_id` (`job_instance_id`),
  CONSTRAINT `job_instance_error_log_ibfk_1` FOREIGN KEY (`job_instance_id`) REFERENCES `job_instance` (`job_instance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1;
