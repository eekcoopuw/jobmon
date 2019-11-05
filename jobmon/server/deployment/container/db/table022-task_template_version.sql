--
-- Table structure for table `task_template_version`
--

USE `docker`;

DROP TABLE IF EXISTS `task_template_version`;

CREATE TABLE `task_template_version`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_template_id` int(11) NOT NULL,
  `command_template` varchar(1000) NOT NULL,
  `arg_mapping_hash` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- Add our foreign key constraints
ALTER TABLE `task_template_version` ADD CONSTRAINT `fk_tt_version_task_template_id` FOREIGN KEY (`task_template_id`) REFERENCES `docker`.`task_template` (`id`);
