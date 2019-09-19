--
-- Table structure for table `workflow_attribute`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_attribute`;
CREATE TABLE `workflow_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  `submitted_date_short` date,
  PRIMARY KEY (`id`, `submitted_date_short`),
  KEY `workflow_id` (`workflow_id`),
  KEY `attribute_type` (`attribute_type`)
  ) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (TO_DAYS(submitted_date_short))
( PARTITION p201908 VALUES LESS THAN (TO_DAYS('2019-09-01'))ENGINE = InnoDB,
PARTITION p201909 VALUES LESS THAN (TO_DAYS('2019-10-01'))ENGINE = InnoDB,
PARTITION p201910 VALUES LESS THAN (TO_DAYS('2019-11-01'))ENGINE = InnoDB,
PARTITION p201911 VALUES LESS THAN (TO_DAYS('2019-12-01'))ENGINE = InnoDB,
PARTITION p201912 VALUES LESS THAN (TO_DAYS('2020-01-01'))ENGINE = InnoDB,
PARTITION p202001 VALUES LESS THAN (TO_DAYS('2020-02-01'))ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB

)*/;