--
-- Table structure for table `queue`
--
use `docker`;

DROP TABLE IF EXISTS `queue`;

CREATE TABLE `queue` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `cluster_id` INTEGER NOT NULL,
  `parameters` VARCHAR(2500) NOT NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

ALTER TABLE `queue` ADD UNIQUE INDEX `ui_name_cluster_id` (`name`, `cluster_id`);
ALTER TABLE `queue` ADD INDEX `ix_cluster_id` (`cluster_id`);

-- load queue names
LOCK TABLES `cluster` c READ, `cluster_type` ct READ, `queue` WRITE;


INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'null.q', c.id, '{}'
AS `parameters`
FROM cluster c
    INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'sequential';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'null.q', c.id, '{"cores": 1}'
AS `parameters`
FROM cluster c
    INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'dummy';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'null.q', c.id, '{"cores": 20}'
AS `parameters`
FROM cluster c
    INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'multiprocess';

UNLOCK TABLES;
