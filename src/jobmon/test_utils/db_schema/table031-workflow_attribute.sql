--
-- Table structure for table `task_attribute`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_attribute`;
CREATE TABLE `workflow_attribute` (
  `workflow_id` int(11) NOT NULL,
  `workflow_attribute_type_id` int(11) NOT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`workflow_id`, `workflow_attribute_type_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
