--
-- Table structure for table `arg`
--

DROP TABLE IF EXISTS `arg`;

CREATE TABLE `arg`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- create our unique constraints
ALTER TABLE `arg` ADD CONSTRAINT `uc_arg` UNIQUE (`name`);
