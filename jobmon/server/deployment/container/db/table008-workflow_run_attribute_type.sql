--
-- Table structure for table `workflow_run_attribute_type`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_run_attribute_type`;
CREATE TABLE `workflow_run_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

LOCK TABLES `workflow_run_attribute_type` WRITE;
INSERT INTO `workflow_run_attribute_type` VALUES (1,'NUM_LOCATIONS','int'),(2,'NUM_DRAWS','int'),(3,'NUM_AGE_GROUPS','int'),(4,'NUM_YEARS','int'),(5,'NUM_RISKS','int'),(6,'NUM_CAUSES','int'),(7,'NUM_SEXES','int'),(8,'TAG','string'),(9,'NUM_MEASURES','int'),(10,'NUM_METRICS','int'),(11,'NUM_MOST_DETAILED_LOCATIONS','int'),(12,'NUM_AGGREGATE_LOCATIONS','int'),(13,'SLOT_LIMIT_AT_START','int'),(14,'SLOT_LIMIT_AT_END','int');
UNLOCK TABLES;
