--
-- Table structure for table `arg`
--
use `docker`;

DROP TABLE IF EXISTS `arg`;

CREATE TABLE `arg`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- create our unique constraints
ALTER TABLE `arg` ADD CONSTRAINT `uc_arg` UNIQUE (`name`);
