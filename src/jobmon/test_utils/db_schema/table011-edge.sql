--
-- Table structure for table `edge`
--
use `docker`;

DROP TABLE IF EXISTS `edge`;
CREATE TABLE `edge` (
  `dag_id` INTEGER NOT NULL,
  `node_id` INTEGER NOT NULL,
  `upstream_node_ids` longtext,
  `downstream_node_ids` longtext,
  PRIMARY KEY (`dag_id`, `node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
