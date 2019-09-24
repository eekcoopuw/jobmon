--
-- Table structure for table `job_attribute`
--
use `docker`;

DROP TABLE IF EXISTS `job_attribute`;
CREATE TABLE `job_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `job_id` (`job_id`),
  KEY `attribute_type` (`attribute_type`),
  CONSTRAINT `job_attribute_ibfk_1` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`),
  CONSTRAINT `job_attribute_ibfk_2` FOREIGN KEY (`attribute_type`) REFERENCES `job_attribute_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;