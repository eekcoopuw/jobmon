--
-- Table structure for table `workflow_attribute_type`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_attribute_type`;
CREATE TABLE `workflow_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `workflow_attribute_type` ADD CONSTRAINT `uc_name` UNIQUE (`name`);
