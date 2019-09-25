--
-- Table structure for table `workflow_run_attribute`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_run_attribute`;
CREATE TABLE `workflow_run_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_run_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `workflow_run_id` (`workflow_run_id`),
  KEY `attribute_type` (`attribute_type`),
  CONSTRAINT `workflow_run_attribute_ibfk_1` FOREIGN KEY (`workflow_run_id`) REFERENCES `workflow_run` (`id`),
  CONSTRAINT `workflow_run_attribute_ibfk_2` FOREIGN KEY (`attribute_type`) REFERENCES `workflow_run_attribute_type` (`id`)
) ENGINE=InnoDB;