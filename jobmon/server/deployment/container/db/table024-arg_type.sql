--
-- Table structure for table `args`
--
use `docker`;

DROP TABLE IF EXISTS `arg_type`;

CREATE TABLE `arg_type`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- load defaults
LOCK TABLES `arg_type` WRITE;
INSERT INTO `arg_type` VALUES (1, 'NODE_ARG'), (2, 'TASK_ARG'), (3, 'OP_ARG');
UNLOCK TABLES;

