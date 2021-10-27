--
-- Table structure for table `command`
--

DROP TABLE IF EXISTS `command`;

CREATE TABLE `command`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `command_type_id` INTEGER NOT NULL,
  `tool_id` INTEGER NOT NULL,
  `name` varchar(250) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- add index on our fake foreign key
ALTER TABLE `command` ADD INDEX `ix_command_type_id` (`command_type_id`);
ALTER TABLE `command` ADD INDEX `ix_tool_id` (`tool_id`);

-- add unique constraint
ALTER TABLE `command` ADD CONSTRAINT `uc_tool_command_name` UNIQUE (`tool_id`, `command_type_id`, `name`);
