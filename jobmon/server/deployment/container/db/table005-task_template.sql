--
-- Table structure for table `task_template`
--

USE `docker`;

DROP TABLE IF EXISTS `task_template`;

CREATE TABLE `task_template`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tool_version_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- Add our foreign key constraints
ALTER TABLE `task_template` ADD CONSTRAINT `fk_task_template_tool_version_id` FOREIGN KEY (`tool_version_id`) REFERENCES `docker`.`tool_version` (`id`);

-- create our unique constraints
ALTER TABLE `task_template` ADD CONSTRAINT `uc_tool_version_id_name` UNIQUE (`tool_version_id`, `name`);
