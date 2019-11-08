--
-- Table structure for table `job_instance`
--
use `docker`;

DROP TABLE IF EXISTS `tool`;

CREATE TABLE `tool`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `tool` ADD CONSTRAINT `uc_name` UNIQUE (`name`);

LOCK TABLES `tool` WRITE;
INSERT INTO `tool` VALUES (1,'unknown');
UNLOCK TABLES;

