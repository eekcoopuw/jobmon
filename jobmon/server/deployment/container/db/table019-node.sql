--
-- Table structure for table `node`
--
use `docker`;

DROP TABLE IF EXISTS `node`;
CREATE TABLE `node` (
  `id` INTEGER NOT NULL,
  `dag_id` INTEGER NOT NULL,
  `task_template_version_id` INTEGER NOT NULL,
  `node_arg_hash` INTEGER NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;