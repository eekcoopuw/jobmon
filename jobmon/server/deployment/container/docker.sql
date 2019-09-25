--
-- Use database `docker`
--
USE `docker`;

--
-- Table structure for table `job_instance_status`
--
DROP TABLE IF EXISTS `job_instance_status`;
CREATE TABLE `job_instance_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `job_status`
--
DROP TABLE IF EXISTS `job_status`;
CREATE TABLE `job_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `job_attribute_type`
--
DROP TABLE IF EXISTS `job_attribute_type`;
CREATE TABLE `job_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=latin1;

--
-- Table structure for table `executor_parameter_set_type`
--
DROP TABLE IF EXISTS `executor_parameter_set_type`;
CREATE TABLE `executor_parameter_set_type` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_attribute_type`
--
DROP TABLE IF EXISTS `workflow_attribute_type`;
CREATE TABLE `workflow_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_run_attribute_type`
--
DROP TABLE IF EXISTS `workflow_run_attribute_type`;
CREATE TABLE `workflow_run_attribute_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_run_status`
--
DROP TABLE IF EXISTS `workflow_run_status`;
CREATE TABLE `workflow_run_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_status`
--
DROP TABLE IF EXISTS `workflow_status`;
CREATE TABLE `workflow_status` (
  `id` varchar(1) NOT NULL,
  `label` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `task_dag`
--
DROP TABLE IF EXISTS `task_dag`;
CREATE TABLE `task_dag` (
  `dag_id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_hash` varchar(150) DEFAULT NULL,
  `name` varchar(150) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `heartbeat_date` datetime DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  PRIMARY KEY (`dag_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15496 DEFAULT CHARSET=latin1;

--
-- Table structure for table `job`
--
DROP TABLE IF EXISTS `job`;
CREATE TABLE `job` (
  `job_id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_id` int(11) DEFAULT NULL,
  `job_hash` varchar(255) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `tag` varchar(255) DEFAULT NULL,
  `command` text,
  `executor_parameter_set_id` int(11) DEFAULT NULL,
  `num_attempts` int(11) DEFAULT NULL,
  `max_attempts` int(11) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  PRIMARY KEY (`job_id`),
  KEY `ix_dag_id_status_date` (`dag_id`,`status_date`),
  KEY `ix_job_status_date` (`status_date`),
  KEY `executor_parameter_set_id` (`executor_parameter_set_id`),
  KEY `status` (`status`),
  CONSTRAINT `job_ibfk_2` FOREIGN KEY (`status`) REFERENCES `job_status` (`id`),
  CONSTRAINT `job_ibfk_3` FOREIGN KEY (`dag_id`) REFERENCES `task_dag` (`dag_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2011549 DEFAULT CHARSET=latin1;

--
-- Table structure for table `executor_parameter_set`
--
DROP TABLE IF EXISTS `executor_parameter_set`;
CREATE TABLE `executor_parameter_set` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) NOT NULL,
  `parameter_set_type` varchar(1) NOT NULL,
  `max_runtime_seconds` int(11) DEFAULT NULL,
  `context_args` varchar(1000) DEFAULT NULL,
  `resource_scales` varchar(1000) DEFAULT NULL,
  `queue` varchar(255) DEFAULT NULL,
  `num_cores` int(11) DEFAULT NULL,
  `m_mem_free` float DEFAULT NULL,
  `j_resource` tinyint(1) DEFAULT NULL,
  `hard_limits` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `parameter_set_type` (`parameter_set_type`),
  KEY `job_id` (`job_id`),
  CONSTRAINT `executor_parameter_set_ibfk_1` FOREIGN KEY (`parameter_set_type`) REFERENCES `executor_parameter_set_type` (`id`),
  CONSTRAINT `executor_parameter_set_ibfk_2` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4088858 DEFAULT CHARSET=latin1;

--
-- Table structure for table `job_instance`
--
DROP TABLE IF EXISTS `job_instance`;
CREATE TABLE `job_instance` (
  `job_instance_id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_run_id` int(11) DEFAULT NULL,
  `executor_type` varchar(50) DEFAULT NULL,
  `executor_id` int(11) DEFAULT NULL,
  `job_id` int(11) NOT NULL,
  `dag_id` int(11) NOT NULL,
  `executor_parameter_set_id` int(11) NOT NULL,
  `usage_str` varchar(250) DEFAULT NULL,
  `nodename` varchar(50) DEFAULT NULL,
  `process_group_id` int(11) DEFAULT NULL,
  `wallclock` varchar(50) DEFAULT NULL,
  `maxrss` varchar(50) DEFAULT NULL,
  `cpu` varchar(50) DEFAULT NULL,
  `io` varchar(50) DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  `submitted_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `report_by_date` datetime DEFAULT NULL,
  PRIMARY KEY (`job_instance_id`),
  KEY `ix_job_instance_dag_id` (`dag_id`),
  KEY `ix_job_instance_executor_id` (`executor_id`),
  KEY `executor_parameter_set_id` (`executor_parameter_set_id`),
  KEY `status` (`status`),
  KEY `job_id` (`job_id`),
  CONSTRAINT `job_instance_ibfk_1` FOREIGN KEY (`dag_id`) REFERENCES `task_dag` (`dag_id`),
  CONSTRAINT `job_instance_ibfk_2` FOREIGN KEY (`executor_parameter_set_id`) REFERENCES `executor_parameter_set` (`id`),
  CONSTRAINT `job_instance_ibfk_3` FOREIGN KEY (`status`) REFERENCES `job_instance_status` (`id`),
  CONSTRAINT `job_instance_ibfk_4` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1804346 DEFAULT CHARSET=latin1;

--
-- Table structure for table `job_attribute`
--
DROP TABLE IF EXISTS `job_attribute`;
CREATE TABLE `job_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `job_id` (`job_id`),
  KEY `attribute_type` (`attribute_type`),
  CONSTRAINT `job_attribute_ibfk_1` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`),
  CONSTRAINT `job_attribute_ibfk_2` FOREIGN KEY (`attribute_type`) REFERENCES `job_attribute_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8901069 DEFAULT CHARSET=latin1;

--
-- Table structure for table `job_instance_error_log`
--
DROP TABLE IF EXISTS `job_instance_error_log`;
CREATE TABLE `job_instance_error_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_instance_id` int(11) NOT NULL,
  `error_time` datetime DEFAULT NULL,
  `description` text,
  PRIMARY KEY (`id`),
  KEY `job_instance_id` (`job_instance_id`),
  CONSTRAINT `job_instance_error_log_ibfk_1` FOREIGN KEY (`job_instance_id`) REFERENCES `job_instance` (`job_instance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=184858 DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow`
--
DROP TABLE IF EXISTS `workflow`;
CREATE TABLE `workflow` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_id` int(11) DEFAULT NULL,
  `workflow_args` text,
  `workflow_hash` text,
  `description` text,
  `name` varchar(150) DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `dag_id` (`dag_id`),
  KEY `status` (`status`),
  CONSTRAINT `workflow_ibfk_1` FOREIGN KEY (`dag_id`) REFERENCES `task_dag` (`dag_id`),
  CONSTRAINT `workflow_ibfk_2` FOREIGN KEY (`status`) REFERENCES `workflow_status` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15484 DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_run`
--
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
  `resource_adjustment` float DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `status_date` datetime DEFAULT NULL,
  `status` varchar(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `workflow_id` (`workflow_id`),
  KEY `status` (`status`),
  CONSTRAINT `workflow_run_ibfk_1` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`),
  CONSTRAINT `workflow_run_ibfk_2` FOREIGN KEY (`status`) REFERENCES `workflow_run_status` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16891 DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_attribute`
--
DROP TABLE IF EXISTS `workflow_attribute`;
CREATE TABLE `workflow_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `workflow_id` (`workflow_id`),
  KEY `attribute_type` (`attribute_type`),
  CONSTRAINT `workflow_attribute_ibfk_1` FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`id`),
  CONSTRAINT `workflow_attribute_ibfk_2` FOREIGN KEY (`attribute_type`) REFERENCES `workflow_attribute_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `workflow_run_attribute`
--
DROP TABLE IF EXISTS `workflow_run_attribute`;
CREATE TABLE `workflow_run_attribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_run_id` int(11) DEFAULT NULL,
  `attribute_type` int(11) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `workflow_run_id` (`workflow_run_id`),
  KEY `attribute_type` (`attribute_type`),
  CONSTRAINT `workflow_run_attribute_ibfk_1` FOREIGN KEY (`workflow_run_id`) REFERENCES `workflow_run` (`id`),
  CONSTRAINT `workflow_run_attribute_ibfk_2` FOREIGN KEY (`attribute_type`) REFERENCES `workflow_run_attribute_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=30642 DEFAULT CHARSET=latin1;

--
-- Insert status and types
--
LOCK TABLES `job_instance_status` WRITE;
INSERT INTO `job_instance_status` VALUES ('B','SUBMITTED_TO_BATCH_EXECUTOR'),('D','DONE'),('E','ERROR'),('I','INSTANTIATED'),('R','RUNNING'),('U','UNKNOWN_ERROR'),('W','NO_EXECUTOR_ID'),('Z','RESOURCE_ERROR');
UNLOCK TABLES;
LOCK TABLES `job_status` WRITE;
INSERT INTO `job_status` VALUES ('A','ADJUSTING_RESOURCES'),('D','DONE'),('E','ERROR_RECOVERABLE'),('F','ERROR_FATAL'),('G','REGISTERED'),('I','INSTANTIATED'),('Q','QUEUED_FOR_INSTANTIATION'),('R','RUNNING');
UNLOCK TABLES;
LOCK TABLES `workflow_run_status` WRITE;
INSERT INTO `workflow_run_status` VALUES ('D','DONE'),('E','ERROR'),('R','RUNNING'),('S','STOPPED');
UNLOCK TABLES;
LOCK TABLES `workflow_status` WRITE;
INSERT INTO `workflow_status` VALUES ('C','CREATED'),('D','DONE'),('E','ERROR'),('R','RUNNING'),('S','STOPPED');
UNLOCK TABLES;
LOCK TABLES `executor_parameter_set_type` WRITE;
INSERT INTO `executor_parameter_set_type` VALUES ('A','ADJUSTED'),('O','ORIGINAL'),('V','VALIDATED');
UNLOCK TABLES;
LOCK TABLES `job_attribute_type` WRITE;
INSERT INTO `job_attribute_type` VALUES (1,'NUM_LOCATIONS','int'),(2,'NUM_DRAWS','int'),(3,'NUM_AGE_GROUPS','int'),(4,'NUM_YEARS','int'),(5,'NUM_RISKS','int'),(6,'NUM_CAUSES','int'),(7,'NUM_SEXES','int'),(8,'TAG','string'),(9,'NUM_MEASURES','int'),(10,'NUM_METRICS','int'),(11,'NUM_MOST_DETAILED_LOCATIONS','int'),(12,'NUM_AGGREGATE_LOCATIONS','int'),(13,'WALLCLOCK','string'),(14,'CPU','string'),(15,'IO','string'),(16,'MAXRSS','string'),(17,'USAGE_STR','string');
UNLOCK TABLES;
LOCK TABLES `workflow_attribute_type` WRITE;
INSERT INTO `workflow_attribute_type` VALUES (1,'NUM_LOCATIONS','int'),(2,'NUM_DRAWS','int'),(3,'NUM_AGE_GROUPS','int'),(4,'NUM_YEARS','int'),(5,'NUM_RISKS','int'),(6,'NUM_CAUSES','int'),(7,'NUM_SEXES','int'),(8,'TAG','string'),(9,'NUM_MEASURES','int'),(10,'NUM_METRICS','int'),(11,'NUM_MOST_DETAILED_LOCATIONS','int'),(12,'NUM_AGGREGATE_LOCATIONS','int');
UNLOCK TABLES;
LOCK TABLES `workflow_run_attribute_type` WRITE;
INSERT INTO `workflow_run_attribute_type` VALUES (1,'NUM_LOCATIONS','int'),(2,'NUM_DRAWS','int'),(3,'NUM_AGE_GROUPS','int'),(4,'NUM_YEARS','int'),(5,'NUM_RISKS','int'),(6,'NUM_CAUSES','int'),(7,'NUM_SEXES','int'),(8,'TAG','string'),(9,'NUM_MEASURES','int'),(10,'NUM_METRICS','int'),(11,'NUM_MOST_DETAILED_LOCATIONS','int'),(12,'NUM_AGGREGATE_LOCATIONS','int'),(13,'SLOT_LIMIT_AT_START','int'),(14,'SLOT_LIMIT_AT_END','int');
UNLOCK TABLES;

