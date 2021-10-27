--
-- Table structure for table `tool`
--

DROP TABLE IF EXISTS `tool`;

CREATE TABLE `tool`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- create our unique constraints
ALTER TABLE `tool` ADD CONSTRAINT `uc_name` UNIQUE (`name`);
