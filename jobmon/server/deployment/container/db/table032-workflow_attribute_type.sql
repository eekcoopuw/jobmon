--
-- Table structure for table `task_attribute_type`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_attribute_type`;
CREATE TABLE `workflow_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  CONSTRAINT unique_names UNIQUE (`name`),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;
