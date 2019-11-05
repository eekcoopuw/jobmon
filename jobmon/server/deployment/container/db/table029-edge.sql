--
-- Table structure for table `edge`
--
use `docker`;

DROP TABLE IF EXISTS `edge`;
CREATE TABLE `edge` (
  `dag_id` INTEGER NOT NULL,
  `node_id` INTEGER NOT NULL,
  `upstream_nodes` text,
  `downstream_nodes` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8;