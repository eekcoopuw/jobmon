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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- add index on our fake foreign key
ALTER TABLE `node` ADD INDEX `ix_task_template_version_id` (`task_template_version_id`);

-- add constraint on task_template_version_id/node_args_hash
ALTER TABLE `node` ADD CONSTRAINT `uc_task_template_version_id_node_args_hash` UNIQUE (`task_template_version_id`, `node_args_hash`);
