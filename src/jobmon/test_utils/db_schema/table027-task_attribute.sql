--
-- Table structure for table `task_attribute`
--
use `docker`;

DROP TABLE IF EXISTS `task_attribute`;
CREATE TABLE `task_attribute` (
  `task_id` int(11) DEFAULT NULL,
  `task_attribute_type_id` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`task_id`, `task_attribute_type_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;
