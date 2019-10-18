--
-- Table structure for table `node_args`
--
use `docker`;

DROP TABLE IF EXISTS `node_args`;
CREATE TABLE `node_args` (
  `node_id` INTEGER NOT NULL,
  `arg_id` INTEGER NOT NULL,
  `val` varchar(255) NOT NULL,
  PRIMARY KEY (`node_id`, `arg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;