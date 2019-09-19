--
-- Table structure for table `job_instance_error_log`
--
use `docker`;

DROP TABLE IF EXISTS `job_instance_error_log`;
CREATE TABLE `job_instance_error_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_instance_id` int(11) NOT NULL,
  `error_time` datetime DEFAULT NULL,
  `description` text,
  `submitted_date_short` date,
  PRIMARY KEY (`id`. `submitted_date_short`),
  KEY `job_instance_id` (`job_instance_id`)
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
