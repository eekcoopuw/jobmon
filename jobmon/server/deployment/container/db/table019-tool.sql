--
-- Table structure for table `job_instance`
--
use `docker`;

DROP TABLE IF EXISTS `tool`;
CREATE TABLE `tool`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tool_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `tool` WRITE;
INSERT INTO `tool` VALUES ('1','unknown');
UNLOCK TABLES;

