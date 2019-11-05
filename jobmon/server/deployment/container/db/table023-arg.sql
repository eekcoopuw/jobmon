--
-- Table structure for table `arg`
--
use `docker`;

DROP TABLE IF EXISTS `arg`;

CREATE TABLE `arg`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- create our unique constraints
ALTER TABLE `arg` ADD CONSTRAINT `uc_arg_name` UNIQUE (`name`);
