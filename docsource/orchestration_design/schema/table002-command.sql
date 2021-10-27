--
-- Table structure for table `command`
--

DROP TABLE IF EXISTS `command_type`;

CREATE TABLE `command_type`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- create our unique constraints
ALTER TABLE `command_type` ADD CONSTRAINT `uc_command_type` UNIQUE (`name`);
