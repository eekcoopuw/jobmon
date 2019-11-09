--
-- Table structure for table `task_template`
--

USE `docker`;

DROP TABLE IF EXISTS `task_template`;

CREATE TABLE `task_template`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `tool_version_id` INTEGER NOT NULL,
  `name` varchar(250) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- add index on our fake foreign key
ALTER TABLE `task_template` ADD INDEX `ix_tool_version_id` (`tool_version_id`);
-- add unique constraint
ALTER TABLE `task_template` ADD CONSTRAINT `uc_tool_version_id_name` UNIQUE (`tool_version_id`, `name`);
