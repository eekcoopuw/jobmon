--
-- Table structure for table `node_arg`
--
use `docker`;

DROP TABLE IF EXISTS `node_arg`;
CREATE TABLE `node_arg` (
  `node_id` INTEGER NOT NULL,
  `arg_id` INTEGER NOT NULL,
  `val` varchar(1000) NOT NULL,
  PRIMARY KEY (`node_id`, `arg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
