--
-- Table structure for table `cluster`
--
use `docker`;

DROP TABLE IF EXISTS `cluster`;

CREATE TABLE `cluster` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `cluster_type_id` INTEGER NOT NULL,
  `connection_parameters` varchar(2500) NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `cluster` ADD UNIQUE INDEX `ui_name` (`name`);
ALTER TABLE `cluster` ADD INDEX `ix_cluster_type_id` (`cluster_type_id`);

-- load defaults
LOCK TABLES `cluster_type` READ, `cluster` WRITE;

INSERT INTO `cluster`(`name`, `cluster_type_id`, `connection_parameters`)
SELECT 'dummy', id, NULL
FROM `cluster_type`
WHERE `name` = 'dummy';

INSERT INTO `cluster`(`name`, `cluster_type_id`, `connection_parameters`)
SELECT 'sequential', id, NULL
FROM `cluster_type`
WHERE `name` = 'sequential';

INSERT INTO `cluster`(`name`, `cluster_type_id`, `connection_parameters`)
SELECT 'multiprocess', id, NULL
FROM `cluster_type`
WHERE `name` = 'multiprocess';

UNLOCK TABLES;
