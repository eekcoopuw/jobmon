--
-- Table structure for table `task_template_version`
--

USE `docker`;

DROP TABLE IF EXISTS `task_template_version`;

CREATE TABLE `task_template_version`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_template_id` INTEGER NOT NULL,
  `arg_mapping_hash` VARCHAR(150) NOT NULL,
  `command_template` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- add index on our fake foreign key
ALTER TABLE `task_template_version` ADD INDEX `ix_task_template_id` (`task_template_id`);
ALTER TABLE `task_template_version` ADD CONSTRAINT `uc_composite_pk` UNIQUE (`task_template_id`, `arg_mapping_hash`, `command_template`);
