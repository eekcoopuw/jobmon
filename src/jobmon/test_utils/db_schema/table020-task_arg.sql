--
-- Table structure for table `task_arg`
--
use `docker`;

DROP TABLE IF EXISTS `task_arg`;

CREATE TABLE `task_arg` (
  `task_id` INTEGER NOT NULL,
  `arg_id` INTEGER NOT NULL,
  `val` varchar(1000) NOT NULL,
  PRIMARY KEY (`task_id`, `arg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
