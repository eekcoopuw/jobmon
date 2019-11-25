--
-- Table structure for table `task_instance_attribute_type`
--
use `docker`;

DROP TABLE IF EXISTS `task_instance_attribute_type`;
CREATE TABLE `task_instance_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `partition_date` timestamp NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (`id`, `partition_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(partition_date))
( PARTITION p201908 VALUES LESS THAN (UNIX_TIMESTAMP('2019-09-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201909 VALUES LESS THAN (UNIX_TIMESTAMP('2019-10-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201910 VALUES LESS THAN (UNIX_TIMESTAMP('2019-11-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201911 VALUES LESS THAN (UNIX_TIMESTAMP('2019-12-01 00:00:00'))ENGINE = InnoDB,
PARTITION p201912 VALUES LESS THAN (UNIX_TIMESTAMP('2020-01-01 00:00:00'))ENGINE = InnoDB,
PARTITION p202001 VALUES LESS THAN (UNIX_TIMESTAMP('2020-02-01 00:00:00'))ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB
)*/;


LOCK TABLES `task_instance_attribute_type` WRITE;
INSERT INTO `task_instance_attribute_type`
VALUES (1,'NUM_LOCATIONS','int'),(2,'NUM_DRAWS','int'),
(3,'NUM_AGE_GROUPS','int'),(4,'NUM_YEARS','int'),(5,'NUM_RISKS','int'),
(6,'NUM_CAUSES','int'),(7,'NUM_SEXES','int'),(8,'TAG','string'),
(9,'NUM_MEASURES','int'),(10,'NUM_METRICS','int'),
(11,'NUM_MOST_DETAILED_LOCATIONS','int'),(12,'NUM_AGGREGATE_LOCATIONS','int'),
(13,'WALLCLOCK','string'),(14,'CPU','string'),(15,'IO','string'),
(16,'MAXRSS','string'),(17, 'MAXPSS', 'string'),(18,'USAGE_STR','string'),
(19, 'DISPLAY_GROUP', 'string');
UNLOCK TABLES;
