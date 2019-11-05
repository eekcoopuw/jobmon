--
-- Table structure for table `node`
--
use `docker`;

DROP TABLE IF EXISTS `node`;
CREATE TABLE `node` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_template_version_id` INTEGER NOT NULL,
  `node_args_hash` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;