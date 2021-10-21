--
-- Table structure for table `args`
--
use `docker`;

DROP TABLE IF EXISTS `arg_type`;

CREATE TABLE `arg_type`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE `arg_type` ADD CONSTRAINT `uc_name` UNIQUE (`name`);

-- load defaults
LOCK TABLES `arg_type` WRITE;
INSERT INTO `arg_type` VALUES (1, 'NODE_ARG'), (2, 'TASK_ARG'), (3, 'OP_ARG');
UNLOCK TABLES;

