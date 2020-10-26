--
-- Table structure for table `task_template_version`
--

USE `docker`;

DROP TABLE IF EXISTS `template_arg_map`;

CREATE TABLE `template_arg_map`(
  `task_template_version_id` INTEGER NOT NULL,
  `arg_id` INTEGER NOT NULL,
  `arg_type_id` INTEGER NOT NULL,
  PRIMARY KEY (`task_template_version_id`, `arg_id`, `arg_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
