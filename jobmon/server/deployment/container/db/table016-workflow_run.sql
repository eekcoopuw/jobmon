--
-- Table structure for table `workflow_run`
--
use `docker`;

DROP TABLE IF EXISTS `workflow_run`;
CREATE TABLE `workflow_run` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_id` int(11) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `hostname` varchar(150) DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `stderr` varchar(1000) DEFAULT NULL,
  `stdout` varchar(1000) DEFAULT NULL,
  `project` varchar(150) DEFAULT NULL,
  `working_dir` varchar(1000) DEFAULT NULL,
  `slack_channel` varchar(150) DEFAULT NULL,
  `executor_class` varchar(150) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `partition_date` timestamp NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (`id`, `partition_date`),
  KEY `workflow_id` (`workflow_id`),
  KEY `status` (`status`)
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
