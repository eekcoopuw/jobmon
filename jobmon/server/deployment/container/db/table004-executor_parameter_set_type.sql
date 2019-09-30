--
-- Table structure for table `executor_parameter_set_type`
--
use `docker`;

DROP TABLE IF EXISTS `executor_parameter_set_type`;
CREATE TABLE `executor_parameter_set_type` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `executor_parameter_set_type` WRITE;
INSERT INTO `executor_parameter_set_type` VALUES ('A','ADJUSTED'),('O','ORIGINAL'),('V','VALIDATED');
UNLOCK TABLES;