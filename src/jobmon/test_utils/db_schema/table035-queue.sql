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
SELECT 'sequential', c.id,
'{}'
AS `parameters`
FROM cluster c
    INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'sequential';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'all.q', c.id,
'{''max_threads'': 102, ''memory'': (0.128, 1010), ''default_runtime_seconds'': 24 * 60 * 60, ''max_runtime_seconds'': 3 * 24 * 60 * 60}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'long.q', c.id,
'{''max_threads'': 102, ''memory'': (0.128, 1010), ''default_runtime_seconds'': 24 * 60 * 60, ''max_runtime_seconds'': 16 * 24 * 60 * 60}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'geospatial.q', c.id,
'{''max_threads'': 62, ''memory'': (0.128, 1010), ''default_runtime_seconds'': 24 * 60 * 60, ''max_runtime_seconds'': 25 * 24 * 60 * 60}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'i.q', c.id,
'{''max_threads'': 78, ''memory'': (0.128, 750), ''default_runtime_seconds'': 24 * 60 * 60, ''max_runtime_seconds'': 7 * 24 * 60 * 60}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'd.q', c.id,
'{''max_threads'': 78, ''memory'': (0.128, 750), ''default_runtime_seconds'': 24 * 60 * 60, ''max_runtime_seconds'': 7 * 24 * 60 * 60}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

UNLOCK TABLES;
