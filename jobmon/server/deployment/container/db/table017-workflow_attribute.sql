--
-- Table structure for table `workflow_attribute`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_attribute`;
CREATE TABLE `workflow_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `workflow_id` (`workflow_id`),
  KEY `attribute_type` (`attribute_type`),
  CONSTRAINT `workflow_attribute_ibfk_1` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`),
  CONSTRAINT `workflow_attribute_ibfk_2` FOREIGN KEY (`attribute_type`) REFERENCES `workflow_attribute_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;