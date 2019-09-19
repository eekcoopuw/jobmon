--
-- Table structure for table `task_dag`
--
use `docker`;

DROP TABLE IF EXISTS `task_dag`;
CREATE TABLE `task_dag` (
  `dag_id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_hash` varchar(150) DEFAULT NULL,
  `name` varchar(150) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `heartbeat_date` datetime DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `submitted_date_short` date,
  PRIMARY KEY (`dag_id`, `submitted_date_short`)
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