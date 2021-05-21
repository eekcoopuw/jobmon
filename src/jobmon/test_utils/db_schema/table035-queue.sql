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
'{''cores'': (1, 102), ''memory'': (0.128, 1010), ''runtime'': (10, 259200)}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'long.q', c.id,
'{''cores'': (1, 102), ''memory'': (0.128, 1010), ''runtime'': (10, 1382400)}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'geospatial.q', c.id,
'{''cores'': (1, 62), ''memory'': (0.128, 1010), ''runtime'': (10, 2160000)}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'i.q', c.id,
'{''cores'': (1, 78), ''memory'': (0.128, 750), ''runtime'': (10, 604800)}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
SELECT 'd.q', c.id,
'{''cores'': (1, 78), ''memory'': (0.128, 750), ''runtime'': (10, 604800)}'
AS `parameters`
FROM cluster c
	INNER JOIN cluster_type ct ON c.cluster_type_id = ct.id
WHERE ct.name = 'UGE';

UNLOCK TABLES;
