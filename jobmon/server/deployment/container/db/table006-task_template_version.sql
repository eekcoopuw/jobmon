--
-- Table structure for table `task_template_version`
--

USE `docker`;

DROP TABLE IF EXISTS `task_template_version`;

CREATE TABLE `task_template_version`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `task_template_id` INTEGER NOT NULL,
  `command_template` text NOT NULL,
  `arg_mapping_hash` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- add index on our fake foreign key
ALTER TABLE `task_template_version` ADD INDEX `ix_task_template_id` (`task_template_id`);
