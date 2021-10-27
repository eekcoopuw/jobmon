--
-- Table structure for table `node`
--

DROP TABLE IF EXISTS `node`;

CREATE TABLE `node` (
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `node_args_hash` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- add constraint on node_args_hash
ALTER TABLE `node` ADD CONSTRAINT `uc_node_args_hash` UNIQUE (`node_args_hash`);
