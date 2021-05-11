--
-- Table structure for table `job_instance`
--
use `docker`;

DROP TABLE IF EXISTS `tool_version`;

CREATE TABLE `tool_version`(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `tool_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- add index on our fake foreign key
ALTER TABLE `tool_version` ADD INDEX `ix_tool_id` (`tool_id`);

LOCK TABLES `tool_version` WRITE;
INSERT INTO `tool_version` VALUES (1,1);
UNLOCK TABLES;

