--
-- Table structure for table `job_instance`
--
use `docker`;

DROP TABLE IF EXISTS `tool_version`;
CREATE TABLE `tool_version`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tool_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_tool_version_tool_id` FOREIGN KEY (`tool_id`) REFERENCES `docker`.`tool` (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `tool_version` WRITE;
INSERT INTO `tool_version` VALUES (1,1);
UNLOCK TABLES;

